use std::{
    collections::{HashMap, HashSet},
    io::Write,
    path::PathBuf,
};

use crate::classify::Classifier;
use crate::error::Error;
use crate::lang::LangFiles;
use crate::lang::LANG;
use itertools::Itertools;
use rayon::prelude::*;
use ungoliant::wet::Wet;
use warc::RawRecord;

pub struct Pipeline {
    src: PathBuf,
    dst: PathBuf,
}
// temporary
// will be changed to enum
// #[derive(PartialEq, Eq, Hash, Debug)]
// struct Lang(String);

#[derive(Debug)]
struct ShardContent {
    pub inner: HashMap<&'static str, HashSet<String>>,
}

/// holds a hashmap over (lang, sentences)
/// stored into a HashMap<Lang, HashSet<String>>
/// TODO: use a view rather than a copy?
impl ShardContent {
    pub fn new() -> Self {
        ShardContent {
            inner: HashMap::new(),
        }
    }

    /// insert a sentence
    pub fn insert(&mut self, sentence: String, lang: &'static str) -> bool {
        // check existence of a HashSet for provided Lang
        // if not, create HashSet and insert value
        if let Some(sentences) = self.inner.get_mut(&lang) {
            sentences.insert(sentence)
        } else {
            let mut hs = HashSet::new();
            let ret = hs.insert(sentence);
            self.inner.insert(lang, hs);
            ret
        }
    }
}

impl Pipeline {
    pub fn new(src: PathBuf, dst: PathBuf) -> Self {
        Pipeline { src, dst }
    }

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
        let mut langfiles = LangFiles::new(&self.dst).unwrap();

        results.for_each(|(idx, wetfile)| {
            // TODO: Use sorted_sentences.clear() to reuse same memory space
            let mut sorted_sentences = ShardContent::new();

            info!("processing shard {:?}", idx);

            for record in wetfile {
                let record = record.unwrap();
                let body = String::from_utf8(record.body).ok();
                if let Some(sentences) = body {
                    for sentence in sentences.lines().filter(|line| line.chars().count() > 100) {
                        let prediction = cls.predict(&sentence).ok();
                        if let Some(Some(lang)) = prediction {
                            let lang = lang.get(0).unwrap();
                            let lang = lang.label.clone();
                            let lang = LANG.get(lang.as_str()).unwrap();
                            sorted_sentences.insert(sentence.to_string(), *lang);
                        }
                    }
                }
            }

            debug!("writing shard {:?} into lang files", idx);
            for (lang, sentences) in sorted_sentences.inner {
                debug!("writing to lang {}", lang);
                let mut fd = langfiles.get(&lang).unwrap();
                let content = sentences.into_iter().join("\n");
                fd.write_all(&content.as_bytes()).unwrap();
            }
        });
        Ok(())
    }
}
