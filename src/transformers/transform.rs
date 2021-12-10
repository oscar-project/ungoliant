//! Transform trait.

use std::ops::RangeInclusive;

use warc::BufferedBody;

use crate::pipelines::oscardoc::types::Document;

pub trait Transform<T> {
    /// Takes ownership of [Document] and returns it.
    fn transform(&self, doc: &mut T) -> Vec<RangeInclusive<usize>>;
}
