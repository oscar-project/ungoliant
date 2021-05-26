use std::{
    collections::{HashMap, HashSet},
    io::Write,
    ops::{Range, RangeInclusive},
    path::PathBuf,
    vec::IntoIter,
};

use crate::classify::Classifier;
use crate::error::Error;
use crate::lang::LangFiles;
use crate::lang::LANG;
use crate::pipeline::pipeline::Pipeline;
use itertools::Itertools;
use rayon::prelude::*;
use std::hash::BuildHasherDefault;
use twox_hash::XxHash64;
use ungoliant::shard::wet::Wet;
use warc::{header::WarcHeader, RawRecord};

pub struct RayonAll {
    src: PathBuf,
    dst: PathBuf,
    with_metadata: bool,
}

type WarcHeaders = HashMap<WarcHeader, Vec<u8>>;

/// container for (lang, sentences) pairs
#[derive(Debug)]
struct ShardContent {
    pub inner: HashMap<&'static str, Vec<String>, BuildHasherDefault<XxHash64>>,
}

impl ShardContent {
    /// create a new, empty [ShardContent]. Uses [Default::default] for initialization
    pub fn new() -> Self {
        ShardContent {
            inner: Default::default(),
        }
    }

    /// inserts `sentence` into `lang` vector
    ///
    /// Creates `lang` vector if non existent
    pub fn insert(&mut self, sentence: String, lang: &'static str) -> () {
        if let Some(sentences) = self.inner.get_mut(&lang) {
            sentences.push(sentence)
        } else {
            let mut sentences = Vec::new();
            let ret = sentences.push(sentence);
            self.inner.insert(lang, sentences);
            ret
        }
    }
}

/// Processing pipeline.
///
/// May be changed into a Trait to allow for more implementation flexibility
impl RayonAll {
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
}

impl Pipeline<()> for RayonAll {
    fn run(&self) -> Result<(), Error> {
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

        // iterate over shards
        results.for_each(|(idx, shard)| {
            let mut sorted_sentences = ShardContent::new();
            info!("processing shard {:?}", idx);

            // convert into a parallel iterator
            let wetfile = shard.enumerate().par_bridge();

            let shard_results: Vec<(Vec<(String, &'static str)>, WarcHeaders)> = wetfile
                .filter_map(|(idx_record, record)| match record {
                    Ok(record) => RayonAll::process_record(record, &cls),
                    Err(e) => {
                        warn!("Error on record {} of shard {}: {}", idx_record, idx, e);
                        return None;
                    }
                })
                // collect here is blocking
                // because we can't write concurrently into a HashMap
                // and using Mutexes might ruin performance.
                .collect(); //TODO: test with a for_each and a channel to send?

            //TODO continue
            //shows records where there's more than one language detected.
            for (sentences, header) in shard_results[..300].iter() {
                let langs: Vec<&&str> = sentences.iter().map(|(_, lang)| lang).collect();
                let grouped = Self::group_by(langs);
                if grouped.len() > 1 {
                    println!("{:#?}", sentences);
                    println!("{:#?}", grouped);
                }
            }

            // store predictions into sorted_sentences
            if !self.with_metadata {
                for (record, _) in shard_results {
                    record
                        .into_iter()
                        .for_each(|(sentence, lang)| sorted_sentences.insert(sentence, lang));
                }

                // write to disk
                debug!("writing shard {:?} into lang files", idx);
                for (lang, sentences) in sorted_sentences.inner {
                    let mut fd = langfiles.get(&lang).unwrap();
                    let content = sentences.into_iter().join("\n");
                    fd.write_all(&content.as_bytes()).unwrap();
                    println!("{:?}", fd);
                }
            } else {
                let mut offsets: HashMap<&str, usize> = HashMap::new();

                // iterate over records
                for (record, header) in shard_results {
                    // holds references to identified languages of each sentence
                    //TODO: see if we can spare the copy here
                    let langs: Vec<&str> = record.iter().map(|(_, lang)| lang.clone()).collect();
                    let sentences: Vec<String> =
                        record.into_iter().map(|(sentences, _)| sentences).collect();
                    // chunk references by langid
                    let chunks = Pipeline::group_by(langs);

                    println!(
                        "{:?}",
                        String::from_utf8_lossy(header.get(&WarcHeader::RecordID).unwrap())
                    );

                    // write sentences for each identified language
                    for (lang, ranges) in chunks {
                        let mut fd = langfiles.get(lang).unwrap();

                        // sums ranges for each identified language
                        // this way we know which offset to provide for next iteration
                        let nb_sentences = ranges
                            .iter()
                            .fold(0, |acc, x| acc + x.end() - x.start() + 1);

                        // register/bump offsets
                        match offsets.get_mut(lang) {
                            Some(offset) => *offset += nb_sentences,
                            None => {
                                offsets.insert(lang, nb_sentences);
                            }
                        };

                        println!("\t{:?}: {:?} ({:?} sen.)", lang, ranges, nb_sentences);
                        let mut sen = String::new();
                        for range in ranges {
                            sen += &sentences[range].join("\n");
                        }
                        fd.write_all(&mut sen.as_bytes()).unwrap();
                    }
                }
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

        let r = Pipeline::group_by(langs);
        println!("expected: {:?}", &expected);
        println!("result  : {:?}", &r);
        for (k, v) in r {
            assert_eq!(&v, expected.get(k).unwrap());
        }
    }

    #[test]
    fn group_by_empty() {
        let langs: Vec<&str> = Vec::new();

        let r = Pipeline::group_by(langs);
        assert!(r.is_empty());
    }

    #[test]
    fn group_by_uniq() {
        let langs = vec!["fr"; 10];

        let r = Pipeline::group_by(langs);
        let mut expected: HashMap<&str, Vec<RangeInclusive<usize>>> = HashMap::new();
        expected.insert("fr", vec![0..=9]);
        assert_eq!(r, expected);
    }

    #[test]
    fn group_by_uniq_but_first() {
        let mut langs = vec!["fr"; 10];
        langs.insert(0, "it");

        let r = Pipeline::group_by(langs);
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

        let r = Pipeline::group_by(langs);
        let mut expected: HashMap<&str, Vec<RangeInclusive<usize>>> = HashMap::new();
        expected.insert("fr", vec![0..=9]);
        expected.insert("it", vec![10..=10]);
        println!("{:?}", r);
        assert_eq!(r, expected);
    }
}
