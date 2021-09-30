use warc::RawRecordHeader;

use crate::lang::Lang;

/// OSCAR-specific metadata
struct Metadata {
    identification: Lang,
    sentence_identifications: Vec<Lang>,
}
/// A Document is a structure holding content, WARC headers and OSCAR-specific metadata.
struct Document {
    content: String,
    warc_headers: RawRecordHeader,
    metadata: Metadata,
}

impl Document {
    pub fn new(content: String, warc_headers: RawRecordHeader, metadata: Metadata) -> Document {
        Self {
            content,
            warc_headers,
            metadata,
        }
    }
}
