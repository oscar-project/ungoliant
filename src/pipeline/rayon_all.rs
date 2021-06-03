//! Concurrent pipeline using rayon on shard, record and sentence scope.
//!
//! produces a corpus identical to OSCAR 2018
use std::{
    borrow::BorrowMut,
    collections::{hash_map::Entry, HashMap, HashSet},
    io::Write,
    ops::{Range, RangeInclusive},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    vec::IntoIter,
};

use crate::lang::LANG;
use crate::pipeline::pipeline::Pipeline;
use crate::{error::Error, lang::LangFiles};
use itertools::Itertools;
use rayon::prelude::*;
use std::hash::BuildHasherDefault;
use twox_hash::XxHash64;
use ungoliant::{classify::Classifier, shard::wet::Wet};
use warc::{header::WarcHeader, RawRecord};

/// pipeline-specific functions and [Pipeline] implementation.
pub struct RayonAll {
    src: PathBuf,
    dst: PathBuf,
}

/// convinience type for WARC headers.
type WarcHeaders = HashMap<WarcHeader, Vec<u8>>;

/// container for (lang, sentences) pairs.
#[derive(Debug)]
struct ShardContent {
    pub inner: HashMap<&'static str, Vec<String>, BuildHasherDefault<XxHash64>>,
}

impl ShardContent {
    /// create a new, empty [ShardContent]. Uses [Default::default] for initialization.
    pub fn new() -> Self {
        ShardContent {
            inner: Default::default(),
        }
    }

    /// inserts `sentence` into `lang` vector.
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
/// _Shards, records and sentences are processed concurrently._
///
/// for each sentence that is >100 chars, we identify language, retaining
/// identification if top prediction is of a >0.80 confidence.
///
/// Then, we group sentences by lang and write them on disk.
/// Writes are done at the end of processing of each shard.
impl RayonAll {
    /// Instantiate a new pipeline.
    ///
    /// - [RayonAll::src] must exist and contain `n.txt.gz` files
    /// - [RayonAll::dst] must exist and will contain language files
    /// Be aware that no checks are done regarding path validity or existence.
    pub fn new(src: PathBuf, dst: PathBuf) -> Self {
        Self { src, dst }
    }

    /// Process a provided record, by discarding headers
    /// and sentences that do not meet the criteria
    ///
    /// then groups identified sentences by language.
    fn process_record(record: RawRecord, cls: &Classifier) -> Option<Vec<(String, &'static str)>> {
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

            Some(results)
        } else {
            None
        }
    }
}

impl Pipeline<()> for RayonAll {
    /// run the [RayonAll] pipeline.
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

            let shard_results: Vec<Vec<(String, &'static str)>> = wetfile
                .filter_map(|(idx_record, record)| match record {
                    Ok(record) => RayonAll::process_record(record, &cls),
                    Err(e) => {
                        warn!("Error on record {} of shard {}: {}", idx_record, idx, e);
                        return None;
                    }
                })
                .collect(); //TODO: test with a for_each and a channel to send?

            // store predictions into sorted_sentences
            for record in shard_results {
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
        });

        Ok(())
    }
}
