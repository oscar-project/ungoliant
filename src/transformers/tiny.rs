use crate::pipelines::oscardoc::types::Document;

use super::Annotate;

pub struct TinyDocument {
    threshold: usize,
}
impl Annotate for TinyDocument {
    fn annotate(&self, doc: &mut Document) {
        if doc.content().lines().count() < self.threshold {
            doc.metadata_mut().set_annotation("tiny".to_string())
        }
    }
}

impl Default for TinyDocument {
    fn default() -> Self {
        Self { threshold: 5 }
    }
}
