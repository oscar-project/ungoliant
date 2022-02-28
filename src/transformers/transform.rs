//! Transform trait.

use std::ops::RangeInclusive;

pub trait Transform<T> {
    /// Takes ownership of [Document] and returns it.
    fn transform(&self, doc: &mut T) -> Vec<RangeInclusive<usize>>;
}
