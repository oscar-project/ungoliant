//! Annotate trait

/// Annotations provide contextual information about content.
pub trait Annotate<T> {
    fn annotate(&self, doc: &mut T);
}

/// Annotator enables annotation chaining, adding multiple annotators and
/// doing the annotation process in one step.
pub struct Annotator<T>(Vec<Box<dyn Annotate<T> + Sync>>);

impl<T> Annotator<T> {
    pub fn add(&mut self, annotator: Box<dyn Annotate<T> + Sync>) -> &mut Annotator<T> {
        self.0.push(annotator);
        self
    }
}
impl<T> Annotate<T> for Annotator<T> {
    fn annotate(&self, doc: &mut T) {
        for annotator in &self.0 {
            annotator.annotate(doc);
        }
    }
}

impl<T> Default for Annotator<T> {
    fn default() -> Self {
        Self(vec![])
    }
}

#[cfg(test)]
mod tests {
    use crate::{pipelines::oscardoc::types::Document, transformers::Annotate};

    use super::Annotator;

    #[test]
    fn test_default() {
        let a = Annotator::<Document>::default();
        assert_eq!(a.0.len(), 0);
    }

    #[test]
    fn test_add() {
        struct MockAnnotate {}
        impl Annotate<Document> for MockAnnotate {
            fn annotate(&self, _: &mut Document) {}
        }

        let mut a = Annotator::default();
        a.add(Box::new(MockAnnotate {}));

        assert_eq!(a.0.len(), 1);
    }
}
