/*! Filtering utilities

Filters can operate on sentence or record level.

Filters implement [filter::Filter], [filter::FilterMut] or both:
- [filter::Filter] is implemented for filters that do not have state (see [sentence::Length] for example)
- [filter::FilterMut] is implemented for filter that do have state (that is, detection constraints can evolve through time).

Both can be implemented for a given filter,
in order to provide a mutable detection that could be used to "train" the filter, then an immutable one to effectively filter content.
! */
mod filter;
mod record;
mod sentence;

pub use filter::Filter;
pub use filter::FilterMut;
