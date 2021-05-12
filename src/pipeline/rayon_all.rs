use std::{
    collections::{HashMap, HashSet},
    io::Write,
    ops::Range,
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
                }
            } else {
                for (record, header) in shard_results {
                    Self::link_metadata(record, header);
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
    fn link_metadata() {
        // we should have 4 "paragraphs"
        let sentences = vec![
            ("Hello, how are you?", "en"),
            ("Bonjour, comment allez-vous?", "fr"),
            ("Je vais bien merci, et vous?", "fr"),
            ("Yo soy un gaucho, pero no vivo a la pampa.", "es"),
            ("This is a new paragraph from the same record.", "en"),
        ]
        .into_iter()
        .map(|(sen, lang)| (sen.to_string(), *LANG.get(lang).unwrap()))
        .collect();

        let mut header: WarcHeaders = HashMap::new();
        header.insert(WarcHeader::ContentLength, vec![0]);
        Pipeline::link_metadata(sentences, header);
    }
    #[test]
    fn link_metadata_2() {
        // we should have 3 "paragraphs"
        let sentences = vec![
            ("Hello, how are you?", "en"),
            ("Bonjour, comment allez-vous?", "fr"),
            ("Je vais bien merci, et vous?", "fr"),
            ("Yo soy un gaucho, pero no vivo a la pampa.", "es"),
            ("Donde esta la biblioteca?", "es"),
        ]
        .into_iter()
        .map(|(sen, lang)| (sen.to_string(), *LANG.get(lang).unwrap()))
        .collect();

        let mut header: WarcHeaders = HashMap::new();
        header.insert(WarcHeader::ContentLength, vec![0]);
        Pipeline::link_metadata(sentences, header);
    }
}
