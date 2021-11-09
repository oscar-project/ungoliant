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

/// The idea is to take surrounding sentence length into account.
/// With a dumb filter, the following length sentence: `1 1 1 100 1 1 1 1 1 100 100 100 100 100 1 1 1 1`
/// would keep `100 1 1 1 1 1 100 100 100 100 100`, but we'd like to only get `100 100 100 100 100`.
/// The first idea is to iterate over windows, and to sum then divide by window size, keeping the same indices
/// and then filter out.
/// Basically, we convolve with a [... 1/3 1/3 1/3 ...] filter.
/// We could try having filters that are more sensible at the start?
pub struct Conv {
    conv_size: usize,
    rss: RemoveShortSentences,
}

impl Conv {
    pub fn new(conv_size: usize, rss: RemoveShortSentences) -> Self {
        Self { conv_size, rss }
    }

    pub fn transform_idx(&self, mut doc: Document) -> (Document, Vec<RangeInclusive<usize>>) {
        let lines: Vec<&str> = doc.content().lines().collect();
        let line_lengths: Vec<f32> = lines.iter().map(|line| line.len() as f32).collect();
        //add padding
        let padding_size = self.conv_size.div_euclid(2);
        let padding_val_start = line_lengths.first().unwrap().clone();
        let padding_val_end = line_lengths.last().unwrap().clone();

        let mut line_lengths = [
            vec![padding_val_start; padding_size],
            line_lengths,
            vec![padding_val_end; padding_size],
        ]
        .concat();
        //end add padding
        let convolved_lengths: Vec<f32> = line_lengths
            .windows(self.conv_size)
            .map(|lengths| lengths.iter().sum::<f32>() / self.conv_size as f32)
            .collect();

        // filter/remove lines
        // this iterator contains (line_index, (line, convolved_length)).
        // let i: Vec<(usize, (&&str, &f32))> = lines
        let i = lines.iter().zip(convolved_lengths.iter()).enumerate();

        let min_length = *self.rss.filter_min_length() as f32;

        // skip beginning sentences
        let i: Vec<_> = i
            .skip_while(|(_, (_, convolved_length))| convolved_length < &&min_length)
            .collect();

        // skip ending sentences
        let i: Vec<_> = i
            .into_iter()
            .rev()
            .skip_while(|(_, (_, convolved_length))| convolved_length < &&min_length)
            .collect();

        match (i.first(), i.last()) {
            (Some(end), Some(start)) => {
                let ranges = vec![start.0..=end.0];
                let sentences = i
                    .into_iter()
                    .rev()
                    .map(|(_, (sentence, _))| sentence)
                    .join("\n");

                doc.set_content(sentences);
                (doc, ranges)
            }
            _ => (doc, Vec::new()),
        }
        // println!("{:?}", line_lengths);
        // (doc, Vec::new())
    }
}

impl Default for Conv {
    fn default() -> Self {
        Self {
            conv_size: 5,
            rss: Default::default(),
        }
    }
}
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

    use super::{Conv, RemoveShortSentences};

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

    fn gen_valid_long() -> (Document, String) {
        let content = r"foo
bar
baz
very long sentence about cookies, privacy and blah blah blah blah blah blah blah blah
xx
xx
xx
xx
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
tiny sentence in main content
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
again
hehe
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
xx
xx
xx
xx
xx
xx
very long sentence about cookies, privacy and blah blah blah blah blah blah blah blah
xx
xx
"
        .to_string();

        let expected_content = r"MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
tiny sentence in main content
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
again
hehe
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT 
MAIN CONTENT  MAIN CONTENT MAIN CONTENT MAIN CONTENT MAIN CONTENT"
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

    #[test]
    fn test_conv() {
        let (doc, expected) = gen_valid_long();
        let c = Conv::new(5, RemoveShortSentences::new(60));
        println!(
            "{:?}",
            doc.content()
                .lines()
                .map(|line| line.len())
                .collect::<Vec<usize>>()
        );
        println!("{:#?}", c.transform_idx(doc));
    }
}
