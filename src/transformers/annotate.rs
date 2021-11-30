//! Annotate trait
use super::header::Header;
use super::ContentDetector;
use super::ShortSentences;
use crate::pipelines::oscardoc::types::Document;

/// Annotations provide contextual information about content.
pub trait Annotate {
    fn annotate(&self, doc: &mut Document);
}

/// Annotator enables annotation chaining, adding multiple annotators and
/// doing the annotation process in one step.
pub struct Annotator(Vec<Box<dyn Annotate + Sync>>);

impl Annotate for Annotator {
    fn annotate(&self, doc: &mut Document) {
        for annotator in &self.0 {
            annotator.annotate(doc);
        }
    }
}

impl Default for Annotator {
    fn default() -> Self {
        Self(vec![
            Box::new(ShortSentences::default()),
            Box::new(ContentDetector::with_defaults().unwrap()),
            Box::new(Header::default()),
        ])
    }
}
