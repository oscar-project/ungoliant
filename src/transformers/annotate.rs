//! Annotate trait
use super::header::Header;
use super::noisy::Noisy;
use super::ContentDetector;
use super::ShortSentences;
use super::TinyDocument;
use crate::pipelines::oscardoc::types::Document;

/// Annotations provide contextual information about content.
pub trait Annotate {
    fn annotate(&self, doc: &mut Document);
}

/// Annotator enables annotation chaining, adding multiple annotators and
/// doing the annotation process in one step.
pub struct Annotator(Vec<Box<dyn Annotate + Sync>>);

impl Annotator {
    pub fn add(&mut self, annotator: Box<dyn Annotate + Sync>) -> &mut Annotator {
        self.0.push(annotator);
        self
    }
}
impl Annotate for Annotator {
    fn annotate(&self, doc: &mut Document) {
        for annotator in &self.0 {
            annotator.annotate(doc);
        }
    }
}

impl Default for Annotator {
    fn default() -> Self {
        Self(vec![])
    }
}

#[cfg(test)]
mod tests {
    use crate::transformers::Annotate;

    use super::Annotator;

    #[test]
    fn test_default() {
        let a = Annotator::default();
        assert_eq!(a.0.len(), 0);
    }

    #[test]
    fn test_add() {
        struct MockAnnotate {}
        impl Annotate for MockAnnotate {
            fn annotate(&self, _: &mut crate::pipelines::oscardoc::types::Document) {}
        }

        let mut a = Annotator::default();
        a.add(Box::new(MockAnnotate {}));

        assert_eq!(a.0.len(), 1);
    }
}
