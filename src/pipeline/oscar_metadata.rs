use std::{
    collections::HashMap,
    io::Write,
    ops::RangeInclusive,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use crate::lang::LangFiles;
use crate::lang::LANG;
use crate::{classify::Classifier, metadata};
use crate::{error::Error, metadata::Metadata};
use rayon::prelude::*;
use ungoliant::shard::wet::Wet;
use warc::{header::WarcHeader, RawRecord};

pub struct OscarMetadata {
    src: PathBuf,
    dst: PathBuf,
}

type WarcHeaders = HashMap<WarcHeader, Vec<u8>>;

/// Processing pipeline.
///
/// May be changed into a Trait to allow for more implementation flexibility
impl OscarMetadata {
    pub fn new(src: PathBuf, dst: PathBuf) -> Self {
        Self { src, dst }
    }

    /// Process a provided record.
    fn process_record(
        record: RawRecord,
        cls: &Classifier,
    ) -> Option<(Vec<(String, &'static str)>, WarcHeaders)> {
        let body = String::from_utf8(record.body).ok();

        // process record if body is utf8-valid
        if let Some(sentences) = body {
            // filter out lines that does not contain 100 characters.
            // then convert into a parallel iterator
            let sentences = sentences
                .lines()
                .filter(|line| line.chars().count() > 100)
                .par_bridge();

            let results: Vec<(String, &'static str)> = sentences
                // predict for each sentence, discarding
                // predictions that does not meet threshold
                .filter_map(|sentence| {
                    let prediction = cls.predict(&sentence).ok();

                    if let Some(Some(lang)) = prediction {
                        //TODO: rewrite these two lines more elegantly
                        //      we can unwrap since predict returns None if no predictions are
                        //      found
                        let lang = lang.get(0).unwrap();

                        // check if fasttext provided lang exists
                        // return None if not
                        match LANG.get(lang.label.as_str()) {
                            Some(lang) => Some((sentence.to_string(), *lang)),
                            None => {
                                warn!("lang {} does not exist!", lang.label);
                                return None;
                            }
                        }
                    } else {
                        None
                    }
                })
                .collect();

            Some((results, record.headers))
        } else {
            None
        }
    }

    /// Transforms a list of `values` into a list
    /// of ranges of contiguous sequences of same values.
    /// # Example
    /// ```
    /// let values = vec![1, 1, 2, 2, 1, 3, 3];
    /// let groups = Pipeline::group_by(values);
    /// let mut expected = HashMap::new();
    /// expected.insert(1, vec![0..=1, 4..=4]);
    /// expected.insert(2, vec![2..=3]);
    /// expected.insert(3, vec![5..=6]);
    /// assert_eq!(groups, expected);
    /// ```
    pub fn group_by<T: Eq + std::hash::Hash + Copy>(
        vec: Vec<T>,
    ) -> HashMap<T, Vec<RangeInclusive<usize>>> {
        let nb_sentences = vec.len();
        let mut block_start = 0;
        let mut block_end = nb_sentences;
        let mut cur_group = None;
        let mut ret: HashMap<T, Vec<RangeInclusive<usize>>> = HashMap::new();

        //early return if there's no element
        if nb_sentences == 0 {
            return ret;
        }
        //early return if there's only one element
        if nb_sentences == 1 {
            ret.insert(vec[0], vec![0..=0]);
            return ret;
        }

        // iterate into items from vector
        for (idx, item) in vec.into_iter().enumerate() {
            // see if we've already initiated a chunk
            match cur_group {
                // start first chunk
                None => {
                    block_start = idx;
                    cur_group = Some(item);
                }
                Some(group) => {
                    // if item is not of the same value of group
                    // close current chunk and open another
                    if item != group {
                        block_end = idx - 1;
                        let chunk = block_start..=block_end;
                        // insert or create vec holding chunks
                        // of said language
                        match ret.get_mut(&group) {
                            Some(chunks) => chunks.push(chunk),
                            None => {
                                ret.insert(group, vec![chunk]);
                            }
                        }

                        // set chunk start offset
                        // and current language
                        block_start = idx;
                        cur_group = Some(item);
                    }
                }
            }
        }

        // close last chunk
        block_end = nb_sentences - 1;
        let chunk = block_start..=block_end;
        match cur_group {
            None => println!("???"),
            Some(group) => match ret.get_mut(&group) {
                Some(chunks) => chunks.push(chunk),
                None => {
                    ret.insert(group, vec![chunk]);
                }
            },
        }
        ret
    }

    pub fn process_chunk(
        lang: &'static str,
        sentences: &Vec<String>,
        header: &HashMap<WarcHeader, Vec<u8>>,
        ranges: Vec<RangeInclusive<usize>>,
        offsets: &mut HashMap<&'static str, usize>,
    ) -> (String, &'static str, metadata::Metadata) {
        // sums ranges for each identified language
        // this way we know which offset to provide for next iteration
        let nb_sentences = ranges
            .iter()
            .fold(0, |acc, x| acc + x.end() - x.start() + 1);

        // register/bump offsets
        // and return starting offset of content
        let offset: usize = match offsets.get_mut(lang) {
            Some(off) => {
                *off += nb_sentences;
                *off - nb_sentences
            }
            None => {
                offsets.insert(lang, nb_sentences);
                0
                // nb_sentences
            }
        };

        let mut sen = String::new();
        for range in ranges {
            sen += &sentences[range].join("\n");
            sen += "\n";
        }
        let header_str: HashMap<WarcHeader, String> = header
            .iter()
            .map(|(k, v)| (k.clone(), String::from_utf8_lossy(v).to_string()))
            .collect();
        let meta = metadata::Metadata {
            headers: header_str,
            offset: offset,
            nb_sentences,
        };

        (sen, lang, meta)
    }

    /// Run the whole pipeline
    pub fn run(&self) -> Result<(), Error> {
        let cls = Classifier::new_lid()?;

        // list files in source folder,
        // filter out errors from fs and from gzip/wet.
        // This means that invalid gz files and invalid
        // wet files are discarded silently
        let results = std::fs::read_dir(&self.src)?
            //TODO: log errors!
            //      using ok() silently discards errors
            .filter_map(|shard| shard.ok())
            .filter_map(|shard| Wet::from_path_gzip(&shard.path()).ok());

        // convert to parallel iterator
        // /!\: We use par_bridge, that is suboptimal
        //      compared to implementing IntoParallelIterator
        //      ourselves.
        let results = results.enumerate().par_bridge();

        // holds file handles
        let langfiles = LangFiles::new(&self.dst)?;
        let metafiles = LangFiles::new_meta(&self.dst)?;

        //corpus-scoped line offsets
        let mut offsets_global: Arc<Mutex<HashMap<&'static str, usize>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // iterate over shards
        results.for_each(|(idx, shard)| {
            let mut offsets: HashMap<&str, usize> = HashMap::new();
            let global_offsets = offsets_global.clone();
            // let mut sorted_sentences = ShardContent::new();
            info!("processing shard {:?}", idx);

            // convert into a parallel iterator
            let wetfile = shard.enumerate().par_bridge();

            let shard_results: Vec<(Vec<(String, &'static str)>, WarcHeaders)> = wetfile
                .filter_map(|(idx_record, record)| match record {
                    Ok(record) => OscarMetadata::process_record(record, &cls),
                    Err(e) => {
                        warn!("Error on record {} of shard {}: {}", idx_record, idx, e);
                        return None;
                    }
                })
                // collect here is blocking
                // because we can't write concurrently into a HashMap
                // and using Mutexes might ruin performance.
                .collect(); //TODO: test with a for_each and a channel to send?

            let mut shard_results: Vec<Vec<(String, &'static str, metadata::Metadata)>> =
                shard_results
                    .into_iter()
                    .map(|(record, header)| {
                        // split between langs and sentences
                        let langs: Vec<&str> =
                            record.iter().map(|(_, lang)| lang.clone()).collect();
                        let sentences: Vec<String> =
                            record.into_iter().map(|(sentences, _)| sentences).collect();

                        // chunk references by langid
                        let chunks = OscarMetadata::group_by(langs);

                        // transform vector of chunks (that are (lang, ranges)) into
                        //  (String, Metadata), where Metadata's offset is shard-scoped.
                        let processed_chunks: Vec<(String, &'static str, metadata::Metadata)> =
                            chunks
                                .into_iter()
                                .map(|(lang, ranges)| {
                                    OscarMetadata::process_chunk(
                                        lang,
                                        &sentences,
                                        &header,
                                        ranges,
                                        &mut offsets,
                                    )
                                })
                                .collect();

                        processed_chunks
                    })
                    .collect();

            {
                let mut o = global_offsets.lock().unwrap();

                // update metadata with global offsets
                // TODO: flatten shard_results into a vec of records?
                for record in &mut shard_results {
                    for (_, lang, meta) in record {
                        match o.get(lang) {
                            Some(global_offset) => meta.offset += global_offset,
                            None => (),
                        }
                    }
                }

                // update global offsets
                for (lang, offset) in offsets {
                    match o.get_mut(lang) {
                        Some(g_offset) => *g_offset += offset,
                        None => {
                            o.insert(lang, offset);
                        }
                    }
                }

                //mutex drop due to end of scope
            }

            // group by lang to limit number of writes.
            // TODO use clear instead of new
            let mut sentences_to_write: HashMap<&'static str, String> = HashMap::new();
            let mut metadata_to_write: HashMap<&'static str, Vec<Metadata>> = HashMap::new();

            // flatten to get a vector of 3-uplets (sentences, lang, metadata)
            // instead of having to iterate through records too.
            for (s, l, m) in shard_results.into_iter().flatten() {
                // we use entry API to concatenate or insert string
                // TODO use this wherever it's applicable
                sentences_to_write
                    .entry(l)
                    .and_modify(|v| *v += &s)
                    .or_insert(s);

                // we use a less intuitive approach
                // because of mutability rules
                match metadata_to_write.get_mut(l) {
                    Some(meta) => meta.push(m),
                    None => {
                        metadata_to_write.insert(l, vec![m]);
                    }
                };
            }

            // write into files
            for lang in sentences_to_write.keys() {
                let mut fd = langfiles.get(lang).unwrap();
                let mut fd_meta = metafiles.get(lang).unwrap();

                fd.write_all(sentences_to_write.get(lang).unwrap().as_bytes())
                    .unwrap();
                fd_meta
                    .write_all(
                        serde_json::to_string_pretty(metadata_to_write.get(lang).unwrap())
                            .unwrap()
                            .as_bytes(),
                    )
                    .unwrap();
            }
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn group_by_simple() {
        // simple case
        let langs = vec![
            "en", "en", //
            "fr", "fr", "fr", "fr", //
            "en", "en", //
            "fr", "fr", //
            "es", "es", "es", "es", //
        ];

        let mut expected: HashMap<&str, Vec<RangeInclusive<usize>>> = HashMap::new();
        expected.insert("en", vec![0..=1, 6..=7]);
        expected.insert("fr", vec![2..=5, 8..=9]);
        expected.insert("es", vec![10..=13]);

        let r = OscarMetadata::group_by(langs);
        println!("expected: {:?}", &expected);
        println!("result  : {:?}", &r);
        for (k, v) in r {
            assert_eq!(&v, expected.get(k).unwrap());
        }
    }

    #[test]
    fn group_by_empty() {
        let langs: Vec<&str> = Vec::new();

        let r = OscarMetadata::group_by(langs);
        assert!(r.is_empty());
    }

    #[test]
    fn group_by_uniq() {
        let langs = vec!["fr"; 10];

        let r = OscarMetadata::group_by(langs);
        let mut expected: HashMap<&str, Vec<RangeInclusive<usize>>> = HashMap::new();
        expected.insert("fr", vec![0..=9]);
        assert_eq!(r, expected);
    }

    #[test]
    fn group_by_uniq_but_first() {
        let mut langs = vec!["fr"; 10];
        langs.insert(0, "it");

        let r = OscarMetadata::group_by(langs);
        let mut expected: HashMap<&str, Vec<RangeInclusive<usize>>> = HashMap::new();
        expected.insert("it", vec![0..=0]);
        expected.insert("fr", vec![1..=10]);
        println!("{:?}", r);
        assert_eq!(r, expected);
    }
    #[test]
    fn group_by_uniq_but_last() {
        let mut langs = vec!["fr"; 10];
        langs.push("it");

        let r = OscarMetadata::group_by(langs);
        let mut expected: HashMap<&str, Vec<RangeInclusive<usize>>> = HashMap::new();
        expected.insert("fr", vec![0..=9]);
        expected.insert("it", vec![10..=10]);
        println!("{:?}", r);
        assert_eq!(r, expected);
    }
}
