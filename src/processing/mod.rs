/*! Content processing

Contains structures and functions to transform and aggregate data from sources.

This module is for now only compatible with CommonCrawl extracted content, but will be made generic when it is needed.
!*/
mod chunks;
pub mod document;
pub mod metadata;

pub use document::{Document, MergedPiece, PartChunk};
pub use metadata::Metadata;
