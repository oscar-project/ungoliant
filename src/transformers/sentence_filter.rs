//! Removes short sentences that are before/after a contiguous chunk of the file.
//!
//! The idea is to remove contiguous short sentences that are located before and after a main body.
//!
//! By default the short sentence threshold is at 100
//! Example:
//! ```text
//! foo
//! bar
//! baz
//! xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
//! quux
//! xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
//! xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
//! baz
//! bar
//! foo
//! ```
//!
//! will be transformed into
//!
//! ```text
//! xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
//! quux
//! xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
//! xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
//! ```
use std::ops::RangeInclusive;

use itertools::Itertools;

use crate::{
    filtering::{sentence::Length, Filter},
    pipelines::oscardoc::types::Document,
};

use super::Transform;

pub struct RemoveShortSentences {
    filter: Length,
}

impl RemoveShortSentences {
    /// Use a custom min_length for long sentences.
    fn new(min_length: usize) -> Self {
        Self {
            filter: Length::with_min_size(min_length),
        }
    }

    /// get filter detection threshold
    fn filter_min_length(&self) -> &usize {
        self.filter.min_size()
    }

    pub fn transform_idx(&self, mut doc: Document) -> (Document, Vec<RangeInclusive<usize>>) {
        let lines = doc.content().lines();
        let s: Vec<(usize, &str)> = lines
            .enumerate()
            .skip_while(|(_, sentence)| !self.filter.detect(sentence))
            .collect();

        // do the same thing while reversing the iterator
        // this way, we skip the short sentences at the end
        let s: Vec<(usize, &str)> = s
            .into_iter()
            .rev()
            .skip_while(|(_, sentence)| !self.filter.detect(sentence))
            //.map(|(idx, _)| idx)
            .collect();

        // if we did not get start or end, we return an empty vector
        // meaning that we keed nothing. (analogous to transform_own)
        match (s.first(), s.last()) {
            (Some(end), Some(start)) => {
                let ranges = vec![start.0..=end.0];
                let sentences = s.into_iter().rev().map(|(_, sentence)| sentence).join("\n");

                doc.set_content(sentences);

                (doc, ranges)
            }
            _ => (doc, Vec::new()),
        }
    }
}

impl Transform for RemoveShortSentences {
    fn transform_own(&self, mut doc: Document) -> Document {
        let sentences = doc.content().lines();

        // TODO: find a better way to do this

        // skip while sentences are not long enough at start
        // we collect into Vec because skip_while does not return a DoubleEndedIterator.
        // We should be able to get a DoubleEndedIterator to avoid collecting here.
        let s: Vec<&str> = sentences
            .skip_while(|sentence| !self.filter.detect(sentence))
            .collect();

        // do the same thing while reversing the iterator
        // this way, we skip the short sentences at the end
        let s: Vec<&str> = s
            .into_iter()
            .rev()
            .skip_while(|sentence| !self.filter.detect(sentence))
            .collect();

        let sentences = s.into_iter().rev().join("\n");

        doc.set_content(sentences);

        doc
    }
}

impl Default for RemoveShortSentences {
    fn default() -> Self {
        Self {
            filter: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::pipelines::oscardoc::types::{Document, Metadata};
    use crate::transformers::Transform;

    use super::RemoveShortSentences;

    fn gen_valid() -> (Document, String) {
        let content = r"foo
bar
baz

xxxxxxxxxxx
quux
xxxxxxxxxxx
xxxxxxxxxxx
foo
bar
baz
"
        .to_string();

        let expected_content = r"xxxxxxxxxxx
quux
xxxxxxxxxxx
xxxxxxxxxxx"
            .to_string();
        let headers = HashMap::new();
        let metadata = Metadata::default();
        let doc = Document::new(content, headers, metadata);

        (doc, expected_content)
    }

    fn gen_invalid() -> Document {
        let content = r"foo
bar
baz

quux
foo
bar
baz
"
        .to_string();
        let headers = HashMap::new();
        let metadata = Metadata::default();
        let doc = Document::new(content, headers, metadata);

        doc
    }
    #[test]
    fn test_rss_default() {
        let rss = RemoveShortSentences::default();
        assert_eq!(rss.filter_min_length(), &100);
    }
    #[test]
    fn test_rss() {
        let (doc, expected_content) = gen_valid();
        let rss = RemoveShortSentences::new(10);

        let doc_transformed = rss.transform_own(doc);

        println!("{:#?}", doc_transformed);

        assert_eq!(doc_transformed.content(), &expected_content);
    }

    #[test]
    fn test_rss_empty() {
        let content = r"foo
bar
baz

quux
foo
bar
baz
"
        .to_string();

        let headers = HashMap::new();
        let metadata = Metadata::default();
        let doc = Document::new(content, headers, metadata);

        let rss = RemoveShortSentences::new(10);

        let doc_transformed = rss.transform_own(doc);

        println!("{:#?}", doc_transformed);

        assert_eq!(doc_transformed.content(), "");
    }

    #[test]
    fn test_rss_idx() {
        let (doc, _) = gen_valid();
        let expected_range = vec![4..=7];
        let rss = RemoveShortSentences::new(10);

        let doc_transformed = rss.transform_idx(doc);

        println!("{:#?}", doc_transformed);

        assert_eq!(doc_transformed.1, expected_range);
    }

    #[test]
    fn test_rss_idx_invalid() {
        let doc = gen_invalid();
        let expected_ranges = Vec::new();

        let rss = RemoveShortSentences::new(10);

        let ranges = rss.transform_idx(doc);

        assert_eq!(ranges.1, expected_ranges);
    }
}
