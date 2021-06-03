//! Generates OSCAR 2018-compatible corpora augmented with metadata.
//!
//! OSCAR v1.1 holds identified sentences in `<lang>.txt` files
//! just like OSCAR 2018, along with metadata that are in a separate `<lang>_meta.json`.
//!
//! # Metadata
//!
//! At the CommonCrawl level, each record is composed of a body and headers.
//! These headers are written into `<lang>_meta.json` in JSON format, and offsets are added
//! to be able to retrieve the record text from the `<lang>.txt` file.
//!
//! ## Example
//!
//! ```json
//! {
//!     "headers": {
//!       "warc-identified-content-language": "fra",
//!       "warc-type": "conversion",
//!       "warc-record-id": "<urn:uuid:00000000-0000-0000-0000-000000000000>",
//!       "warc-block-digest": "sha1:7X6XVXEBXADSGELSDQP4P2U5XLAAA5P6",
//!       "warc-target-uri": "https://foo.bar",
//!       "warc-date": "2021-02-24T17:11:25Z",
//!       "content-length": "4463",
//!       "warc-refers-to": "<urn:uuid:00000000-0000-0000-0000-000000000000>",
//!       "content-type": "text/plain"
//!     },
//!     "offset": 34124,
//!     "nb_sentences": 3
//! }
//!```
//!
//! This particular record begins at offset `34124+1` and ends at `34124+4`.
//!
//! # Chunks
//! When processing a record that holds sentences in multiple languages,
//! There is the need to extract each contiguous sequence of sentences that share the same language.
//! Chunks are these contiguous sequences, and the [chunks] module deals with them.
mod chunks;
mod metadata;
pub mod oscar_metadata;

use metadata::Metadata;
