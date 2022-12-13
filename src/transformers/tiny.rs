use crate::pipelines::oscardoc::types::Document;

use super::Annotate;

pub struct TinyDocument {
    threshold: usize,
}
impl Annotate<Document> for TinyDocument {
    fn annotate(&self, doc: &mut Document) {
        if doc.content().lines().count() < self.threshold {
            doc.metadata_mut().add_annotation("tiny".to_string())
        }
    }
}

impl Default for TinyDocument {
    fn default() -> Self {
        Self { threshold: 5 }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{
        pipelines::oscardoc::types::{Document, Metadata},
        transformers::Annotate,
    };

    use super::TinyDocument;

    #[test]
    fn test_annotation() {
        let b = "this is a short
        short document";
        let mut d = Document::new(b.to_string(), HashMap::new(), Metadata::default());

        let annotator = TinyDocument::default();
        annotator.annotate(&mut d);

        assert_eq!(
            d.metadata().annotation(),
            Some(vec!["tiny".to_string()]).as_ref()
        );
    }

    #[test]
    fn test_no_annotation() {
        let b = "this is not a short
        short document
        it has tiny sentences
        but has enough sentences
        or so I think";
        let mut d = Document::new(b.to_string(), HashMap::new(), Metadata::default());

        let annotator = TinyDocument::default();
        annotator.annotate(&mut d);

        assert_eq!(d.metadata().annotation(), None);
    }
}
