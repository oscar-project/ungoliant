//! Shard/WET utils.
//!
//! Mainly exists to wrap warc's library [warc::WarcReader] and an efficient gzip library.
//!
//! [wet::Wet] implements [Iterator] over contained [warc::RawRecord].
pub mod wet;
