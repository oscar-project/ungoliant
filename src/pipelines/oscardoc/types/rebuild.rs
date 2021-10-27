/*! Rebuild file writer/schema
Each (avro) record is an  `(shard_id, array of (shard) records)`.
!*/

use std::{
    collections::HashMap,
    fs::File,
    hash::Hash,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex},
};

use avro_rs::{AvroResult, Codec, Schema, Writer};
use log::error;
use serde::Deserialize;
use serde::Serialize;
use structopt::lazy_static::lazy_static;

use crate::lang::LANG;
use crate::{error::Error, lang::Lang};

use super::{Location, Metadata};

lazy_static! {
    static ref SCHEMA: Schema = {
        let schema = r#"
        {
            "type":"record",
            "name":"shard_index",
            "fields":[
              {
                "name":"shard_id",
                "type":"long"
              },
              {
                "name":"locations",
                "type":{
                  "type":"array",
                  "items":{
                    "type":"record",
                    "name":"record_entry",
                    "fields":[
                      {
                        "name":"shard_id",
                        "type":"long"
                      },
                      {
                        "name":"record_id",
                        "type":"string"
                      },
                      {
                        "name":"line_start",
                        "type":"long"
                      },
                      {
                        "name":"line_end",
                        "type":"long"
                      },
                      {
                        "name":"loc_in_shard",
                        "type":"long"
                      },
                      {
                        "name":"metadata",
                        "type":"record",
                        "fields":[
                          {
                            "name":"identification",
                            "type":"record",
                            "fields":[
                              {
                                "name":"label",
                                "type":"string"
                              },
                              {
                                "name":"prob",
                                "type":"double"
                              }
                            ]
                          },
                          {"name": "annotation", "type":"string"},
                          {
                            "name":"sentence_identifications",
                            "type":"array",
                            "items":[
                              {
                                "name":"label",
                                "type":"string"
                              },
                              {
                                "name":"prob",
                                "type":"double"
                              }
                            ]
                          }
                        ]
                      }
                    ]
                  }
                }
              }
            ]
          }
     "#;
        Schema::parse_str(schema).unwrap()
    };
}

#[derive(Serialize, Deserialize, Debug)]
struct RebuildInformation {
    shard_id: usize,
    record_id: String,
    line_start: usize,
    line_end: usize,
    loc_in_shard: usize,
    metadata: Metadata,
}

impl RebuildInformation {
    pub fn new(location: Location, metadata: Metadata) -> Self {
        Self {
            shard_id: location.shard_id(),
            // TODO: Useless borrow here.
            record_id: location.record_id().to_owned(),
            line_start: location.line_start(),
            line_end: location.line_end(),
            loc_in_shard: location.loc_in_shard(),
            metadata,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct ShardResult {
    shard_id: i64,
    rebuild_info: Vec<RebuildInformation>,
}

impl ShardResult {
    pub fn new(shard_id: i64, locations: Vec<Location>, metadata: Vec<Metadata>) -> Self {
        let rebuild_info = locations
            .into_iter()
            .zip(metadata.into_iter())
            .map(|(loc, meta)| RebuildInformation::new(loc, meta))
            .collect();
        Self {
            shard_id,
            rebuild_info,
        }
    }
}
/// Holds an Avro writer.
pub struct RebuildWriter<'a, T> {
    schema: &'a Schema,
    writer: Writer<'a, T>,
}

impl<'a, T: std::io::Write> RebuildWriter<'a, T> {
    /// Create a new rebuilder.
    pub fn new(schema: &'a Schema, writer: T) -> Self {
        Self {
            schema,
            writer: Writer::with_codec(schema, writer, Codec::Snappy),
        }
    }

    /// Append a single serializable value (`value` must implement [Serialize]).
    pub fn append_ser<S: Serialize>(&mut self, value: S) -> AvroResult<usize> {
        self.writer.append_ser(value)
    }

    /// Append from an interator of values, each implementing [Serialize].
    pub fn extend_ser<I, U: Serialize>(&mut self, values: I) -> AvroResult<usize>
    where
        I: IntoIterator<Item = U>,
    {
        self.writer.extend_ser(values)
    }
}

impl<'a> RebuildWriter<'a, File> {
    /// Create a writer on `dst` file.
    /// Errors if provided path already exists.
    pub fn from_path(dst: &Path) -> Result<Self, Error> {
        let schema = &SCHEMA;
        let dest_file = File::create(dst)?;
        Ok(Self::new(schema, dest_file))
    }
}

pub struct RebuildWriters<'a, T>(HashMap<Lang, Arc<Mutex<RebuildWriter<'a, T>>>>);

impl<'a, T> RebuildWriters<'a, T> {
    pub fn get(&'a self, k: &Lang) -> Option<&Arc<Mutex<RebuildWriter<T>>>> {
        self.0.get(k)
    }
}

impl<'a> RebuildWriters<'a, File> {
    #[inline]
    fn forge_dst(dst: &Path, lang: &Lang) -> PathBuf {
        let mut p = PathBuf::from(dst);
        p.push(format!("{}.avro", lang));

        p
    }

    pub fn with_dst(dst: &Path) -> Result<Self, Error> {
        if dst.is_file() {
            error!("rebuild destination must be an empty folder!");
        };

        if dst.read_dir()?.next().is_none() {
            error!("rebuild destination folder must be empty!");
        }

        let ret: Result<HashMap<Lang, Arc<Mutex<RebuildWriter<'_, File>>>>, Error> = LANG
            .iter()
            .map(|lang| {
                let lang = Lang::from_str(lang).unwrap();
                let path = Self::forge_dst(dst, &lang);
                let rw = RebuildWriter::from_path(&path)?;
                let rw_mutex = Arc::new(Mutex::new(rw));
                Ok((lang, rw_mutex))
            })
            .collect();

        Ok(RebuildWriters(ret?))
    }
}
