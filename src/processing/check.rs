//! This module deals with providing data to evaluate generated corpora.
//!
//! It counts occurrences of words and builds a frequency table in order to assert whether a provided corpus follows Zipf's law or not.

use std::{
    collections::HashMap,
    io::{BufRead, Read},
    path::PathBuf,
};

use crate::io::reader::docreader::DocReader;
use csv::Writer;
use itertools::{Itertools, Zip};
use rayon::{
    iter::ParallelIterator,
    str::{ParallelString, SplitWhitespace},
};
use serde::Serialize;

use unicode_segmentation::UnicodeSegmentation;

use crate::error::Error;

/// Zipf counter. Holds word counts (`HashMap<String, u64>`) and the total number of words.
pub struct Zipf {
    counts: HashMap<String, u64>,
    nb_words: u64,
}

/// A serializable entry composed of rank, count, frequency and constant (frequency/rank).
#[derive(Debug, Serialize)]
pub struct ZipfEntry {
    rank: u64,
    count: u64,
    freq: f64,
    constant: f64,
}

impl ZipfEntry {
    pub fn new(rank: u64, count: u64, freq: f64, constant: f64) -> Self {
        Self {
            rank,
            count,
            freq,
            constant,
        }
    }

    /// Get a reference to the zipf entry's rank.
    pub fn rank(&self) -> u64 {
        self.rank
    }
}

impl Zipf {
    pub fn new() -> Self {
        Self {
            counts: HashMap::default(),
            nb_words: 0,
        }
    }

    /// Convinience function to add 1 to a word count.
    /// Creates the entry if the word is not counted yet.
    #[inline]
    fn add_in_counts(&mut self, word: &str) {
        self.counts
            .entry(word.to_string().to_lowercase())
            .and_modify(|count| *count += 1)
            .or_insert(1);

        self.nb_words += 1;
    }

    /// Add words from a sentence
    pub fn add_count(&mut self, text: &str) {
        text.unicode_words()
            .for_each(|word| self.add_in_counts(word));
    }

    // Get words and frequencies
    pub fn rank_freq_constant(&self) -> Vec<ZipfEntry> {
        self.counts
            .iter()
            .sorted_by(|a, b| b.1.cmp(&a.1))
            .enumerate()
            .map(|(rank, (_, count))| {
                let rank = rank + 1; // rank starts at 1
                let freq = *count as f64 / self.nb_words as f64;
                let constant = freq * rank as f64;
                ZipfEntry::new(rank.try_into().unwrap(), *count, freq, constant)
            })
            .collect()
    }
}

/// Run a word count on an Oscar Schema 2 corpus, outputting data in a csv located at `dst`.
pub fn check(src: PathBuf, dst: PathBuf) -> Result<(), Error> {
    let mut zipf = Zipf::new();

    let r = DocReader::from_path(&src)?;

    let mut out = csv::WriterBuilder::new().from_path(dst)?;

    for document in r {
        let document = document?;
        zipf.add_count(&document.content());
    }

    let v = zipf.rank_freq_constant();
    // v.sort_by(|a, b| b.rank().cmp(&a.rank()));
    for entry in v {
        out.serialize(entry)?;
    }
    out.flush()?;

    Ok(())
}
#[cfg(test)]
mod tests {
    use std::{collections::HashMap, io::BufReader};

    use super::Zipf;

    #[test]
    fn zipf() {
        let text = "foo bar ////////bar baz baz baz quux quux quux quux.···
        hello :)";
        let test: HashMap<&'static str, u64> = [
            ("foo", 1),
            ("bar", 2),
            ("baz", 3),
            ("quux", 4),
            ("hello", 1),
        ]
        .into_iter()
        .collect();
        let mut z = Zipf::new();
        z.add_count(text);

        for (word, count) in z.counts {
            assert_eq!(&count, test.get(word.as_str()).unwrap());
        }
    }

    #[test]
    fn zipf_freq() {
        let text = "foo bar ////////bar baz baz baz quux quux quux quux.···
        hello :)";
        let test: HashMap<&str, u64> = [
            ("foo", 1),
            ("bar", 2),
            ("baz", 3),
            ("quux", 4),
            ("hello", 1),
        ]
        .into_iter()
        .collect();
        let mut z = Zipf::new();
        z.add_count(text);

        for (k, v) in z.counts {
            assert_eq!(&v, test.get(k.as_str()).unwrap());
        }
    }

    #[test]
    fn zipf_chinese() {
        let text = "第一條
        人人 //////////////";
        let test: HashMap<&str, u64> = [("第", 1), ("一", 1), ("條", 1), ("人", 2)]
            .into_iter()
            .collect();
        let mut z = Zipf::new();
        z.add_count(text);

        for (k, v) in z.counts {
            assert_eq!(&v, test.get(k.as_str()).unwrap());
        }
    }
}
