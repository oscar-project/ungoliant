//! Document-level filtering.
use std::convert::TryFrom;

use warc::{BufferedBody, Record};

use super::sentence::Length;
use super::Filter;
use std::cmp::Ordering;
pub enum FilterKind {
    PFilter(PFilter),
}

impl Default for FilterKind {
    fn default() -> Self {
        FilterKind::PFilter(PFilter::default())
    }
}

impl Filter<&Record<BufferedBody>> for FilterKind {
    fn detect(&self, reader: &Record<BufferedBody>) -> bool {
        match self {
            Self::PFilter(p) => p.detect(reader),
        }
    }
}

/// Filters out documents that doesn't have its content enough in long newline-separated strings.
///
/// For each document, we compute the size (in bytes) of newline-separated strings, that we bucket in two bins
/// depending on their size (<>min_length).
/// If the >min_length bin makes for at least sentence_threshold of the document, we keep it.
pub struct PFilter {
    sentence_threshold: f64,
    sentence_filter: Length,
}

impl PFilter {
    pub fn new(sentence_threshold: f64, sentence_filter: Length) -> Self {
        PFilter {
            sentence_threshold,
            sentence_filter,
        }
    }
}

impl Filter<&Record<BufferedBody>> for PFilter {
    fn detect(&self, reader: &Record<BufferedBody>) -> bool {
        // get newline-separated lines
        let body = String::from_utf8_lossy(reader.body());
        let lines = body.lines();

        // init buckets
        let mut bucket_lower: u32 = 0;
        let mut bucket_upper: u32 = 0;

        for line in lines {
            // we do not use sentence_filter since we'd compute sentence length two times.
            let count = line.chars().count();

            // if count is >= than minimum filter size, we add to upper bucket
            match count.cmp(self.sentence_filter.min_size()) {
                Ordering::Less => bucket_lower += u32::try_from(count).unwrap(),
                Ordering::Equal | Ordering::Greater => {
                    bucket_upper += u32::try_from(count).unwrap()
                }
            }
        }

        // get threshold in bytes
        let threshold = self.sentence_threshold * f64::from(bucket_lower + bucket_upper);

        // if the number of bytes in lower bucket exceeds the threshold's one,
        // the document is rejected.
        //
        // long form:
        // if f64::from(bucket_upper) < threshold {
        //     false
        // } else {
        //     true
        // }
        !(f64::from(bucket_upper) < threshold)
    }
}

impl Default for PFilter {
    /// inits PFilter with a threshold of 0.6 (that means, at least 60% of content is from long sentences)
    /// sentence filter's default long sentence threshold (100 codepoints).
    fn default() -> Self {
        PFilter {
            sentence_threshold: 0.6,
            sentence_filter: Length::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use warc::Record;

    use crate::filtering::Filter;

    use super::PFilter;

    #[test]
    fn test_pfilter_fail() {
        let r = Record::default();
        let body = r#"short sentence
        short sentence
        short sentence
        list entry
        list entry
        list entry
        list entry
        list entry
        list entry
        list entry

        annoyingly long sentence about cookies and consent annoyingly long sentence about cookies and consent annoyingly long sentence about cookies and consent
        "#;

        let r = r.add_body(body);

        let f = PFilter::default();
        assert_eq!(f.detect(&r), false);
    }

    #[test]
    fn test_pfilter_success() {
        let r = Record::default();
        let body = r#"short sentence (title)

        short sentence (subtitle)
        
        Lorem ipsum dolor sit amet, consectetur adipiscing elit. Curabitur sagittis, libero nec varius aliquam, odio tortor commodo leo, quis posuere enim neque et justo. Aliquam sollicitudin magna varius sem cursus volutpat. Fusce accumsan tellus quis tellus sollicitudin tincidunt. Integer ullamcorper euismod ipsum, vel tempor purus scelerisque vel. Aenean eleifend pulvinar consectetur. Morbi eu massa eget ipsum vestibulum gravida. Mauris placerat neque ac tortor vestibulum iaculis. Suspendisse consectetur ex eget enim ultricies bibendum. Nulla non congue mi, a tempus est. Morbi non ante ante.

        Nunc a vulputate orci, et pharetra mi. Aliquam vitae dolor orci. Sed ultrices turpis ligula, sit amet venenatis tellus consectetur non. Sed finibus blandit quam. Curabitur vel blandit tellus, a condimentum nunc. Etiam turpis odio, auctor et nulla id, placerat scelerisque nunc. In egestas elit non elit aliquet luctus. Proin quis aliquet diam. Quisque maximus in orci nec pellentesque. Etiam sodales mi vitae massa euismod laoreet. 

        annoying cookie thingy.
        "#;

        let r = r.add_body(body);

        let f = PFilter::default();
        assert_eq!(f.detect(&r), true);
    }
}
