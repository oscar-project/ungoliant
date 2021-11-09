//! Annotate trait
use crate::pipelines::oscardoc::types::Document;

/// Annotations provide contextual information about content.
pub trait Annotate {
    fn annotate(&self, doc: &mut Document);
}
