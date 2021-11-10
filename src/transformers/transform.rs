//! Transform trait.

use std::ops::RangeInclusive;

use crate::pipelines::oscardoc::types::Document;
pub trait Transform {
    /// Takes ownership of [Document] and returns it.
    fn transform(&self, doc: &mut Document) -> Vec<RangeInclusive<usize>>;
}
