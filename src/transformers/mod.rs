/*! Document transformers.

Transforms documents by adding/removing content or headers.

!*/

mod content_detector;
mod sentence_filter;
mod transform;

pub use content_detector::ContentDetector;
pub use sentence_filter::RemoveShortSentences;
pub use transform::Transform;
