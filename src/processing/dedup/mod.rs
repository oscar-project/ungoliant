/*! Deduplication

This currently only uses [runiq](https://github.com/whitfin/runiq) to check for identical sentences.
!*/
pub(super) mod dedup;

pub use dedup::dedup;
