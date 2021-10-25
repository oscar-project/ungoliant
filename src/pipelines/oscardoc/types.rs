use std::{
    borrow::Cow,
    collections::HashMap,
    convert::{TryFrom, TryInto},
};

use warc::{BufferedBody, Record, WarcHeader};

use crate::{identifiers::Identification, lang::Lang};
use serde::{Deserialize, Serialize};

/// Incomplete location error type.
///
/// uses [LocationKind] to inform which field is missing.
#[derive(Debug, Clone)]
pub struct IncompleteLocation {
    missing: LocationKind,
}

/// enum of the mandatory [Location] fields.
///
// Not very elegant but works for now.
#[derive(Debug, Clone)]
pub enum LocationKind {
    ShardId,
    RecordID,
    LineStart,
    LineEnd,
    LocInShard,
}

/// A partial, still being filled location.
/// Each field shouldn't be filled more than once to
/// guarantee some integrity.
// TODO: Add methods to ensure that we add only once?
#[derive(Clone, Debug, PartialEq)]
pub struct LocationBuilder {
    shard_id: Option<usize>,
    record_id: Option<String>,
    line_start: Option<usize>,
    line_end: Option<usize>,
    loc_in_shard: Option<usize>,
}

impl<'a> LocationBuilder {
    /// Set the partial location's shard id.
    pub fn set_shard_id(&mut self, shard_id: usize) {
        self.shard_id = Some(shard_id);
    }

    /// Set the partial location's record id.
    pub fn set_record_id(&mut self, record_id: String) {
        self.record_id = Some(record_id);
    }

    /// Set the partial location's line start.
    pub fn set_line_start(&mut self, line_start: usize) {
        self.line_start = Some(line_start);
    }

    /// Set the partial location's line end.
    pub fn set_line_end(&mut self, line_end: usize) {
        self.line_end = Some(line_end);
    }

    /// Set the partial location's loc in shard.
    pub fn set_loc_in_shard(&mut self, loc_in_shard: usize) {
        self.loc_in_shard = Some(loc_in_shard);
    }

    /// Builds the location.
    ///
    /// Errors if a field is missing
    pub fn build(self) -> Result<Location, IncompleteLocation> {
        self.try_into()
    }
}

impl<'a> Default for LocationBuilder {
    fn default() -> Self {
        Self {
            shard_id: None,
            record_id: None,
            line_start: None,
            line_end: None,
            loc_in_shard: None,
        }
    }
}

impl<'a> TryFrom<LocationBuilder> for Location {
    type Error = IncompleteLocation;

    fn try_from(value: LocationBuilder) -> Result<Self, Self::Error> {
        let shard_id = value.shard_id.ok_or(IncompleteLocation {
            missing: LocationKind::ShardId,
        })?;

        let record_id = value.record_id.ok_or(IncompleteLocation {
            missing: LocationKind::RecordID,
        })?;

        let line_start = value.line_start.ok_or(IncompleteLocation {
            missing: LocationKind::LineStart,
        })?;
        let line_end = value.line_end.ok_or(IncompleteLocation {
            missing: LocationKind::LineEnd,
        })?;
        let loc_in_shard = value.loc_in_shard.ok_or(IncompleteLocation {
            missing: LocationKind::LocInShard,
        })?;

        Ok(Location {
            shard_id,
            record_id,
            line_start,
            line_end,
            loc_in_shard,
        })
    }
}
/// Links a record id to a set location in a shard:
/// - shard_id is the shard number (ex. 12345.txt.gz)
/// - record_id is the record id :)
/// - line_start/line_end are the boundaries of kept text (inclusive)
/// - loc_in_shard is the record index _in_ shard.
///
/// # Example
/// If we're working on the 10th record of a shard that is shard 100,
/// that the record has 10 lines and we only keep the first 5,
/// We'd get `line_start=0, line_end=4, loc_in_shard=99`.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Location {
    shard_id: usize,
    record_id: String,
    line_start: usize,
    line_end: usize,
    loc_in_shard: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]

/// OSCAR-specific metadata
/// TODO: make it a HashMap
pub struct Metadata {
    identification: Identification,
    annotation: Option<String>,
    sentence_identifications: Vec<Option<Identification>>,
}

impl Metadata {
    pub fn new(
        identification: &Identification,
        sentence_identifications: &[Option<Identification>],
    ) -> Self {
        Metadata {
            identification: identification.clone(),
            annotation: None,
            sentence_identifications: sentence_identifications.to_owned(),
        }
    }

    /// Set the metadata's annotation.
    pub fn set_annotation(&mut self, annotation: Option<String>) {
        self.annotation = annotation;
    }

    /// Get a reference to the metadata's annotation.
    pub fn annotation(&self) -> Option<&String> {
        self.annotation.as_ref()
    }
}

impl Default for Metadata {
    /// default Metadata is English with 1.0 prob,
    /// no annotation and a single english sentence with 1.0 prob.
    fn default() -> Self {
        Self {
            identification: Identification::new(Lang::En, 1.0),
            annotation: None,
            sentence_identifications: vec![Some(Identification::new(Lang::En, 1.0))],
        }
    }
}
pub type WarcHeaders = HashMap<WarcHeader, Vec<u8>>;
pub type WarchHeadersSer = HashMap<WarcHeader, String>;

/// A Document is a structure holding content, WARC headers and OSCAR-specific metadata.
/// - TODO: Change warc_headers from [RawRecordHeader] to [warc::Record] with [warc::EmptyBody]?
/// This way we shouldn't have to parse strings or use unwrap on [RawRecordHeader].
#[derive(Serialize, Deserialize, Clone, PartialEq)]
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

impl Document {
    pub fn new(content: String, warc_headers: WarcHeaders, metadata: Metadata) -> Self {
        Self {
            content,
            warc_headers,
            metadata,
        }
    }

    /// Instantiate a Document from a record and a related metadata.
    pub fn from_record(record: Record<BufferedBody>, metadata: Metadata) -> Self {
        let (header, body) = record.into_raw_parts();
        let content = String::from_utf8_lossy(&body).into_owned();
        let warc_headers = header.headers;

        Self {
            content,
            warc_headers,
            metadata,
        }
    }

    /// Get a reference to the Document's identification
    pub fn identification(&self) -> &Identification {
        &self.metadata.identification
    }

    /// Get a reference to the content
    pub fn content(&self) -> &String {
        &self.content
    }

    /// get warc record id
    pub fn warc_id(&self) -> Cow<str> {
        String::from_utf8_lossy(self.warc_headers.get(&WarcHeader::RecordID).unwrap())
    }

    /// Get a reference to the document's warc headers.
    pub fn warc_headers(&self) -> &WarcHeaders {
        &self.warc_headers
    }

    /// Get a mutable reference to the document's metadata.
    pub(crate) fn metadata_mut(&mut self) -> &mut Metadata {
        &mut self.metadata
    }

    /// Get a reference to the document's metadata.
    pub(crate) fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Set the document's content.
    pub fn set_content(&mut self, content: String) {
        self.content = content;
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

#[cfg(test)]
mod tests {
    use warc::{Record, WarcHeader};

    use super::{Document, Metadata};

    #[test]
    fn test_from_record() {
        let record = Record::default();
        let body = "foo
        bar
        baz";

        let record = record.add_body(body);
        let metadata = Metadata::default();
        let doc = Document::from_record(record.clone(), metadata);

        let (headers, body) = record.into_raw_parts();
        assert_eq!(doc.content(), &String::from_utf8_lossy(&body).into_owned());
        assert_eq!(doc.warc_headers(), &headers.headers);
        assert_eq!(
            doc.warc_id(),
            String::from_utf8_lossy(&headers.headers.get(&WarcHeader::RecordID).unwrap())
                .into_owned()
        );
    }
}
