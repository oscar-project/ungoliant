use std::{borrow::Cow, collections::HashMap};

use warc::{RawRecordHeader, WarcHeader};

use crate::identifiers::Identification;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
/// OSCAR-specific metadata
pub struct Metadata {
    identification: Identification,
    sentence_identifications: Vec<Option<Identification>>,
}

impl Metadata {
    pub fn new(
        identification: &Identification,
        sentence_identifications: &[Option<Identification>],
    ) -> Self {
        Metadata {
            identification: identification.clone(),
            sentence_identifications: sentence_identifications.to_owned(),
        }
    }
}

pub type WarcHeaders = HashMap<WarcHeader, Vec<u8>>;

/// A Document is a structure holding content, WARC headers and OSCAR-specific metadata.
/// - TODO: Change warc_headers from [RawRecordHeader] to [warc::Record] with [warc::EmptyBody]?
/// This way we shouldn't have to parse strings or use unwrap on [RawRecordHeader].

#[derive(Serialize, Deserialize, Clone)]
pub struct Document {
    content: String,
    warc_headers: WarcHeaders,
    metadata: Metadata,
}

impl Document {
    pub fn new(content: String, warc_headers: WarcHeaders, metadata: Metadata) -> Document {
        Self {
            content,
            warc_headers,
            metadata,
        }
    }

    pub fn identification(&self) -> &Identification {
        &self.metadata.identification
    }

    pub fn content(&self) -> &String {
        &self.content
    }
    /// get warc record id
    pub fn warc_id(&self) -> Cow<str> {
        String::from_utf8_lossy(&self.warc_headers.get(&WarcHeader::RecordID).unwrap())
    }
}

/// custom debug implementation that converts:
/// - `headers` from [Vec<u8>] to [String] for easier readablility
/// - `content` from [String] to [Vec<String>] to better diagnose identification
impl std::fmt::Debug for Document {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let headers_pp: HashMap<WarcHeader, String> = self
            .warc_headers
            .iter()
            .map(|(k, v)| (k.clone(), String::from_utf8_lossy(v).to_string()))
            .collect();

        let lines = &self.content.lines().collect::<Vec<&str>>();
        f.debug_struct("Document")
            .field("content", &lines)
            .field("warc_headers", &headers_pp)
            .field("metadata", &self.metadata)
            .finish()
    }
}
