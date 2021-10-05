use std::{borrow::Cow, collections::HashMap};

use warc::{RawRecordHeader, WarcHeader};

use crate::identifiers::Identification;
use serde::{ser::SerializeStruct, Deserialize, Serialize};

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
pub type WarchHeadersSer = HashMap<WarcHeader, String>;

/// A Document is a structure holding content, WARC headers and OSCAR-specific metadata.
/// - TODO: Change warc_headers from [RawRecordHeader] to [warc::Record] with [warc::EmptyBody]?
/// This way we shouldn't have to parse strings or use unwrap on [RawRecordHeader].

#[derive(Serialize, Deserialize, Clone)]
#[serde(from = "DocumentSer", into = "DocumentSer")]
pub struct Document {
    content: String,

    warc_headers: WarcHeaders,
    metadata: Metadata,
}

#[derive(Serialize, Deserialize)]
/// Serializable version of [Document].
struct DocumentSer {
    content: String,
    warc_headers: WarchHeadersSer,
    metadata: Metadata,
}

impl From<Document> for DocumentSer {
    fn from(d: Document) -> Self {
        let warc_headers = d
            .warc_headers
            .into_iter()
            .map(|(k, v)| (k, String::from_utf8_lossy(&v).into_owned()))
            .collect();

        Self {
            content: d.content,
            warc_headers,
            metadata: d.metadata,
        }
    }
}

impl From<DocumentSer> for Document {
    fn from(d: DocumentSer) -> Self {
        let warc_headers = d
            .warc_headers
            .into_iter()
            .map(|(k, v)| (k, v.as_bytes().to_vec()))
            .collect();

        Self {
            content: d.content,
            warc_headers,
            metadata: d.metadata,
        }
    }
}

/// manually implement serialize for WarcHeader that are Vec<u8>
/// See [impl Serialize on serde doc](https://serde.rs/impl-serialize.html).
// impl Serialize for Document {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: serde::Serializer,
//     {
//         let mut state = serializer.serialize_struct("Document", 3)?;
//         let serializable_headers: HashMap<WarcHeader, String> = self
//             .warc_headers
//             .iter()
//             .map(|(k, v)| (k.clone(), String::from_utf8_lossy(&v).to_string()))
//             .collect();
//         state.serialize_field("content", &self.content)?;
//         state.serialize_field("headers", &serializable_headers)?;
//         state.serialize_field("metadata", &self.metadata)?;

//         state.end()
//     }
// }
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
