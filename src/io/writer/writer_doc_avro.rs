//! Avro version of [writer_doc::DocWriter].

use std::{fmt::Debug, fs::File, io::Write, path::Path};

use avro_rs::{Codec, Schema, Writer};
use log::{debug, error};
use serde::Serialize;
use structopt::lazy_static::lazy_static;

use crate::{error::Error, pipelines::oscardoc::types::Document};

use super::WriterTrait;

lazy_static! {
    static ref SCHEMA: Schema = {

      // schema of Identification struct
        let identification_schema = r#"
      {"name":"identification", "type":"record", "fields": [
        {"name": "label", "type":"string"},
        {"name": "prob", "type":"float"}
      ]}
"#;
      // schema of Metadata struct
        let metadata_schema = r#"
{
  "type":"record",
  "name":"metadata_record",
  "fields":[
    {"name":"identification", "type":"identification"},
    {"name":"annotation", "type":["null", {"type": "array", "items":"string"}]},
    {"name": "sentence_identifications", "type":"array", "items":[
      "null",
      "identification"
    ]}
  ]
}
"#;

// let warc_metadata = r#"
// {
//     "type": "record",
//     "name": "warc_record",
//     "fields": [
//        {"name": "warc-refers-to", "type": "string"},
//        {"name": "warc-date", "type": "string"},
//        {"name": "warc-block-digest", "type": "string"},
//        {"name": "warc-type", "type": "string"},
//        {"name": "warc-identified-content-language", "type": "string"},
//        {"name": "content-length", "type": "long"},
//        {"name": "warc-target-uri", "type": "string"},
//        {"name": "warc-record-id", "type": "string"},
//        {"name": "content-type", "type": "string"}
//     ]
// }
// "#;
let warc_metadata = r#"
{
    "type": "map",
    "values": "string",
    "name": "warc_record",
    "default": {}
}
"#;

let document_schema = r#"
{
    "type":"record",
    "name":"document",
    "fields": [
        {"name": "content", "type": "string"},
        {"name":"warc_headers", "type": "warc_record"},
        {"name":"metadata", "type": "metadata_record"}
    ]
}

"#;

// let corpus_schema = r#"{
//   "name":"corpus",
//   "type": "array",
//   "items":"document"
// }"#;

  // schema of ShardResult struct
        Schema::parse_list(&[
            identification_schema,
            metadata_schema,
            warc_metadata,
            document_schema,
            // corpus_schema,
        ])
        .unwrap().last().unwrap()
            .clone()
    };
}
pub struct DocWriterAvro<'a, T>
where
    T: Write,
{
    schema: &'a Schema,
    writer: Writer<'a, T>,
}

impl<'a, T> DocWriterAvro<'a, T>
where
    T: Write,
{
    /// Create a new avro writer from shema, writer and a specified codec.
    fn new(schema: &'a Schema, writer: T, codec: Codec) -> Self {
        let avro_writer = avro_rs::Writer::with_codec(schema, writer, codec);
        Self {
            schema,
            writer: avro_writer,
        }
    }

    pub fn extend_ser<I, U: Serialize>(&mut self, vals: I) -> Result<usize, Error>
    where
        I: IntoIterator<Item = U>,
    {
        self.writer.extend_ser(vals).map_err(|e| e.into())
    }
    pub fn append_ser<S>(&mut self, val: &S) -> Result<usize, Error>
    where
        S: Serialize,
    {
        self.writer.append_ser(val).map_err(|e| e.into())
    }

    pub fn flush(&mut self) -> Result<usize, Error> {
        self.writer.flush().map_err(|e| e.into())
    }

    pub fn schema(&self) -> &Schema {
        self.writer.schema()
    }
}

impl<'a> DocWriterAvro<'a, File> {
    pub fn from_file(path: &Path) -> Result<Self, Error> {
        if path.exists() {
            error!("{:?} already exists!", path);
            Err(std::io::Error::new(std::io::ErrorKind::AlreadyExists, format!("{path:?}")).into())
        } else {
            let fh = File::create(path)?;
            Ok(DocWriterAvro::new(&SCHEMA, fh, Codec::Snappy))
        }
    }
}
impl<'a, T> WriterTrait for DocWriterAvro<'a, T>
where
    T: Write,
{
    type Item = Document;

    fn new(
        dst: &std::path::Path,
        lang: &'static str,
        max_file_size: Option<u64>,
    ) -> Result<Self, crate::error::Error>
    where
        Self: Sized,
    {
        todo!()
    }

    fn write(&mut self, vals: Vec<Self::Item>) -> Result<(), crate::error::Error> {
        self.extend_ser(&vals)?;
        Ok(())
    }

    fn write_single(&mut self, val: &Self::Item) -> Result<(), crate::error::Error> {
        todo!()
    }

    fn close_meta(&mut self) -> Result<(), crate::error::Error> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, io::Cursor};

    use avro_rs::Codec;
    use warc::{EmptyBody, Record, WarcHeader};

    use crate::{
        identifiers::Identification,
        io::writer::WriterTrait,
        lang::Lang,
        pipelines::oscardoc::types::{Document, Metadata},
    };

    use super::{DocWriterAvro, SCHEMA};

    #[test]
    fn test_simple() {
        // create io buf, get schema
        let mut buf = vec![];
        let schema = &SCHEMA;

        // create writer
        let mut aw = DocWriterAvro::new(schema, &mut buf, Codec::Null);

        // input docs
        let mut documents = vec![];

        for i in 0..10i32 {
            //forge document
            let mut content = "foo\nbar\nbaz\nquux".to_string();
            content.push_str(&i.to_string());
            let mut headers = HashMap::new();
            headers.insert(WarcHeader::ContentType, "conversion".as_bytes().to_owned());
            headers.insert(
                WarcHeader::Unknown("warc-identified-language".to_string()),
                "fr".as_bytes().to_owned(),
            );
            let default_id = Identification::new(Lang::En, 1.0);
            let mut metadata = Metadata::new(
                &default_id,
                &vec![Some(default_id.clone()), Some(default_id.clone()), None],
            );
            metadata.set_annotation("adult".to_string());
            let d = Document::new(content, headers, metadata);
            documents.push(d);
        }

        // write docs
        for doc in &documents {
            aw.append_ser(&doc).unwrap();
        }
        aw.flush().unwrap();

        // get from reader
        let mut c = Cursor::new(&mut buf);
        let r = avro_rs::Reader::new(&mut c).unwrap();
        let mut from_avro = vec![];
        for record in r {
            let deserialized: Document = avro_rs::from_value(&record.unwrap()).unwrap();
            from_avro.push(deserialized);
        }

        println!("{from_avro:#?}");
        //check equality
        assert_eq!(documents, from_avro);
    }
}
