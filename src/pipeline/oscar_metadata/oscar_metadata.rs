use std::{
    collections::HashMap,
    io::{Read, Seek, SeekFrom, Write},
    ops::RangeInclusive,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use crate::classify::Classifier;
use crate::error::Error;
use crate::lang::LangFiles;
use crate::lang::LANG;
use crate::pipeline::oscar_metadata::chunks;
use crate::pipeline::oscar_metadata::Metadata;
use log::Level::Debug;
use rayon::prelude::*;
use ungoliant::shard::wet::Wet;
use warc::{header::WarcHeader, RawRecord};

/// OSCAR v1.5 generation pipeline
///
/// OSCAR v1.5 is a retrocompatible corpus
/// enhanced with metadata coming from CommonCrawl.
///
/// The CommonCrawl dump is composed of shards,
/// Each shard is composed of records,
/// Each record is composed of a metadata header and a body containing sentences.
///
/// # Processing
/// _every scope is concurrent, that means green threads are created on shards, records and sentences._
/// - We process each record separately, getting a list of sentence-language pairs, along with metadata from the document.
/// - Once we've treated each record of a given shard, we
///   transform out list of sentence-language pairs into chunks of contiguous same-language sentences
///   and we store shard-level line offsets on metadata.
///   Then we group same-language chunks for each language (on shard-level) and we write on disk.
/// - We also keep track of disk-level line offsets to sync shard-level offsets between writes.
///
/// TODO: Better document this step.
pub struct OscarMetadata {
    src: PathBuf,
    dst: PathBuf,
}

/// convinience type alias for [warc::Record] headers.
type WarcHeaders = HashMap<WarcHeader, Vec<u8>>;

impl OscarMetadata {
    pub fn new(src: PathBuf, dst: PathBuf) -> Self {
        Self { src, dst }
    }

    /// attempt to predict language on provided sentence.
    ///
    /// Returns [None] if no language is detected.
    // why return the sentence itself?
    fn identify_sentence(sentence: &str, cls: &Classifier) -> Option<(String, &'static str)> {
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
                    None
                }
            }
        } else {
            None
        }
    }

    /// Process a provided record.
    ///
    /// Here, sentences that are >100 chars are processed,
    /// and the others are discarded.
    /// See [String::chars::count].
    ///
    /// Then, we identify language for each sentence
    /// and return (sentence, language) along with headers
    /// extracted from the WARC.
    fn process_record(
        record: RawRecord,
        cls: &Classifier,
    ) -> Option<(Vec<(String, &'static str)>, WarcHeaders)> {
        if log_enabled!(Debug) {
            debug!(
                "processing record {}",
                String::from_utf8_lossy(
                    record
                        .headers
                        .get(&WarcHeader::RecordID)
                        .unwrap_or(&Vec::from("no record id".as_bytes()))
                )
            );
        };
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
                .filter_map(|sentence| Self::identify_sentence(sentence, cls))
                .collect();

            Some((results, record.headers))
        } else {
            error!(
                "body not UTF-8 valid: {:?}",
                record.headers.get(&WarcHeader::RecordID)
            );
            None
        }
    }

    /// Run the whole pipeline
    pub fn run(&self) -> Result<(), Error> {
        // let errors;

        let cls = Classifier::new_lid()?;

        // list files in source folder,
        // filter out errors from fs and from gzip/wet.
        // This means that invalid gz files and invalid
        // wet files are discarded silently
        let results = std::fs::read_dir(&self.src)?
            .filter_map(|shard| {
                shard.map_or_else(
                    |e| {
                        error!("error reading shard directory: {}", e);
                        None
                    },
                    Some,
                )
            })
            .filter_map(|shard| {
                Wet::from_path_gzip(&shard.path()).map_or_else(
                    |e| {
                        error!("error reading shard file: {}", e);
                        None
                    },
                    Some,
                )
            });

        // convert to parallel iterator
        // /!\: We use par_bridge, that is suboptimal
        //      compared to implementing IntoParallelIterator
        //      ourselves.
        let results = results.enumerate().par_bridge();

        // holds file handles
        let langfiles = LangFiles::new(&self.dst)?;
        let mut metafiles = LangFiles::new_meta(&self.dst)?;

        //corpus-scoped line offsets
        let offsets_global: Arc<Mutex<HashMap<&'static str, usize>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // put json array starting token
        for metafile in metafiles.values_mut() {
            metafile.write_all(b"[")?;
        }

        // iterate over shards
        results.for_each(|(idx, shard)| {
            let mut offsets: HashMap<&str, usize> = HashMap::new();

            // get an atomic reference to global offsets
            // let offsets_global_arc = offsets_global.clone();
            info!("processing shard {:?}", idx);

            // convert into a parallel iterator
            let wetfile = shard.enumerate().par_bridge();

            let shard_results: Vec<(Vec<(String, &'static str)>, WarcHeaders)> = wetfile
                .filter_map(|(idx_record, record)| match record {
                    Ok(record) => OscarMetadata::process_record(record, &cls),
                    Err(e) => {
                        warn!("Error on record {} of shard {}: {}", idx_record, idx, e);
                        None
                    }
                })
                // collect here is blocking
                // because we can't write concurrently into a HashMap
                // and using Mutexes might ruin performance.
                .collect(); //TODO: test with a for_each and a channel to send?

            let mut shard_results: Vec<Vec<(String, &'static str, Metadata)>> = shard_results
                .into_iter()
                .map(|(record, header)| {
                    // split between langs and sentences
                    let langs: Vec<&str> = record.iter().map(|(_, lang)| lang.clone()).collect();
                    let sentences: Vec<String> =
                        record.into_iter().map(|(sentences, _)| sentences).collect();

                    // chunk references by langid
                    let chunks = chunks::group_by(langs);

                    // transform vector of chunks (that are (lang, ranges)) into
                    //  (String, Metadata), where Metadata's offset is shard-scoped.
                    let processed_chunks: Vec<(String, &'static str, Metadata)> = chunks
                        .into_iter()
                        .map(|(lang, ranges)| {
                            chunks::process_chunk(lang, &sentences, &header, ranges, &mut offsets)
                        })
                        .collect();

                    processed_chunks
                })
                .collect();

            {
                let offsets_global_arc = offsets_global.clone();
                let mut offsets_global_mutex = offsets_global_arc.lock().unwrap();

                // update metadata with global offsets
                // TODO: flatten shard_results into a vec of records?
                for record in &mut shard_results {
                    for (_, lang, meta) in record {
                        if let Some(global_offset) = offsets_global_mutex.get(lang) {
                            meta.offset += global_offset;
                        }
                    }
                }

                // update global offsets
                for (lang, offset) in offsets {
                    match offsets_global_mutex.get_mut(lang) {
                        Some(g_offset) => *g_offset += offset,
                        None => {
                            offsets_global_mutex.insert(lang, offset);
                        }
                    }
                }

                //mutex drop due to end of scope
            }

            // group by lang to limit number of writes.
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
                if let Err(err) = Self::write_sentences(&langfiles, lang, &sentences_to_write) {
                    error!("could not write sentences. {} (shard {})", lang, idx);
                    error!("{:?}", err);
                }

                if let Err(err) = Self::write_metadata(&metafiles, lang, &metadata_to_write) {
                    error!("could not write metadata. {} (shard {})", lang, idx);
                    error!("{:?}", err);
                }
            }
        });

        // put json array end token
        // fix trailing comma
        // TODO: Either change the way the comma is added (special case for first iteratoin)
        //       or derive Serialize and enable use of serialize_seq.
        for error in Self::end_json(&mut metafiles).iter().filter(|x| x.is_err()) {
            error!("error while ending/fixing JSON file: {:?}", error);
        }

        Ok(())
    }

    /// writes sentences into language file
    fn write_sentences(
        langfiles: &LangFiles,
        lang: &'static str,
        sentences: &HashMap<&'static str, String>,
    ) -> Result<(), Error> {
        let mut fd = langfiles
            .get(lang)
            .ok_or_else(|| Error::UnknownLang(lang.to_string()))?;
        let stw = sentences
            .get(lang)
            .ok_or_else(|| Error::UnknownLang(lang.to_string()))?
            .as_bytes();

        fd.write_all(stw)?;
        Ok(())
    }

    /// writes metadata into metadata language file
    fn write_metadata(
        metafiles: &LangFiles,
        lang: &'static str,
        metadata: &HashMap<&'static str, Vec<Metadata>>,
    ) -> Result<(), Error> {
        let mut fd_meta = metafiles
            .get(lang)
            .ok_or_else(|| Error::UnknownLang(lang.to_string()))?;

        let mtw = metadata
            .get(lang)
            .ok_or_else(|| Error::UnknownLang(lang.to_string()))?;

        let mtw = mtw.iter().fold(String::new(), |acc, x| {
            // attempt to serialize metadata
            match serde_json::to_string_pretty(x) {
                Ok(serialized) => acc + &serialized + ",",
                Err(e) => {
                    error!(
                        "could not serialize metadata: {:?}\n{:?}",
                        x.headers.get(&WarcHeader::RecordID),
                        e
                    );

                    acc
                }
            }
        });

        fd_meta.write_all(mtw.as_bytes())?;

        Ok(())
    }

    /// Ends json arrays for each metadata files
    /// and fixes trailing comma to comply
    /// with JSON standard
    fn end_json(metafiles: &mut LangFiles) -> Vec<std::io::Result<()>> {
        let mut results = Vec::new();

        let mut buf = [0];
        let comma = b",";

        // convinience function that corrects the trailing comma
        #[inline]
        fn fix(meta_file: &mut std::fs::File, buf: &mut [u8], comma: &[u8]) -> std::io::Result<()> {
            meta_file.seek(SeekFrom::Current(-1))?;
            meta_file.read_exact(buf)?;
            if buf == comma {
                //rewind after read
                meta_file.seek(SeekFrom::Current(-1))?;
                //write null byte
                meta_file.write_all(b"")?;
            }
            Ok(())
        }

        for meta_file in metafiles.values_mut() {
            // get a character before the end
            // and check if it is the offending comma
            results.push(fix(meta_file, &mut buf, comma));

            // put json array end token
            results.push(meta_file.write_all(b"]"));
        }

        results
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

        let r = chunks::group_by(langs);
        println!("expected: {:?}", &expected);
        println!("result  : {:?}", &r);
        for (k, v) in r {
            assert_eq!(&v, expected.get(k).unwrap());
        }
    }

    #[test]
    fn group_by_empty() {
        let langs: Vec<&str> = Vec::new();

        let r = chunks::group_by(langs);
        assert!(r.is_empty());
    }

    #[test]
    fn group_by_uniq() {
        let langs = vec!["fr"; 10];

        let r = chunks::group_by(langs);
        let mut expected: HashMap<&str, Vec<RangeInclusive<usize>>> = HashMap::new();
        expected.insert("fr", vec![0..=9]);
        assert_eq!(r, expected);
    }

    #[test]
    fn group_by_uniq_but_first() {
        let mut langs = vec!["fr"; 10];
        langs.insert(0, "it");

        let r = chunks::group_by(langs);
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

        let r = chunks::group_by(langs);
        let mut expected: HashMap<&str, Vec<RangeInclusive<usize>>> = HashMap::new();
        expected.insert("fr", vec![0..=9]);
        expected.insert("it", vec![10..=10]);
        println!("{:?}", r);
        assert_eq!(r, expected);
    }
}
