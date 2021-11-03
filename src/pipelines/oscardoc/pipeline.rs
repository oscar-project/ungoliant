//! OSCAR Schema v2 generation pipeline
//!
//! OSCAR Schema v2 is a document-oriented corpus schema
//! enhanced with metadata coming from CommonCrawl.
//!
//! The CommonCrawl dump is composed of shards,
//! Each shard is composed of records,
//! Each record is composed of a metadata header and a body containing sentences.
//!
//! # Processing
//! 1. Each record passes through a quality filter that by default checks the content distribution between
//!   short and long sentences, discarding records where the content is primarly in short sentences. (sentence = newline-separated string)
//! 1. The remaining ones get identified both by line and as a whole (we keep the language that has the most information (=bytes)).
//! 1. We pass the records in the adult content annotator
//! 1. We remove remaining short sentences at start/end[^1]
//! 1. We then write documents in files.
//!
//! [^1]: We should do this after step 1: better efficiency.
use std::fs::File;
use std::path::Path;
use std::{collections::HashMap, path::PathBuf};

use super::types::{Document, Location, Metadata, RebuildWriters};
use crate::error::Error;
use crate::filtering::{record, Filter};
use crate::identifiers::FastText;
use crate::identifiers::{self, Identification, Identifier};
use crate::io::writer::WriterTrait;
use crate::lang::Lang;
use crate::pipelines::oscardoc::types::{LocationBuilder, ShardResult};
use crate::pipelines::pipeline::Pipeline;
use crate::sources::commoncrawl::Wet;
use crate::transformers::{self, Transform};
use log::{debug, error, info, warn};
use rayon::prelude::*;
use warc::BufferedBody;
use warc::{Record, WarcHeader};

use crate::io::LangFilesDoc;
pub struct OscarDoc {
    src: PathBuf,
    dst: PathBuf,
    lid_path: PathBuf,
}

impl OscarDoc {
    pub fn new(src: PathBuf, dst: PathBuf, lid_path: PathBuf) -> Self {
        Self { src, dst, lid_path }
    }

    /// list files in source folder,
    /// filter out errors from fs and from gzip/wet.
    ///
    /// This means that invalid gz files and invalid
    /// wet files are discarded silently
    fn get_paths_iter(&self) -> Result<impl Iterator<Item = PathBuf>, Error> {
        let results = std::fs::read_dir(&self.src)?
            .filter_map(|shard| {
                shard.map_or_else(
                    |e| {
                        error!("error reading shard directory: {}", e);
                        None
                    },
                    Some,
                )
            })
            .map(|shard| shard.path());
        Ok(results)
    }

    fn get_shard_number(shard_path: &Path) -> Result<usize, Error> {
        let shard_number = shard_path.file_stem();
        let shard_number = shard_number
            .and_then(|s| s.to_str())
            .and_then(|s| s.split('.').next())
            .and_then(|s| Some(s.parse::<usize>()));

        match shard_number {
            Some(Ok(sn)) => Ok(sn),
            Some(Err(e)) => Err(Error::Custom(format!("{:?}", e))),
            None => Err(Error::Custom(format!(
                "Couldn't extract shard number from {:?}",
                shard_path
            ))),
        }
    }

    /// Process a shard, returning a [Vec] of [Document].
    fn process_shard(
        shard_path: &Path,
        identifier: &identifiers::FastText,
        filter: Option<record::FilterKind>,
    ) -> Result<(usize, Vec<(Document, Location)>), Error> {
        info!("working on shard: {:?}", shard_path);

        // get shard number
        let shard_id = Self::get_shard_number(shard_path)?;

        let shard = Wet::from_path_gzip(&shard_path)?;
        let record_iter = shard.iter.enumerate().par_bridge();

        // get specified filter or resort to default filter kind
        let f = filter.unwrap_or_else(record::FilterKind::default);

        // get iterator on filtered records.
        // only get records that are valid *and* pass the filter.
        let record_iter = record_iter.filter_map(|(idx, record)| match record {
            Ok(r) => {
                if f.detect(&r) {
                    Some((idx, r))
                } else {
                    None
                }
            }
            Err(e) => {
                error!("{:?}", e);
                None
            }
        });

        // begin creation of location
        // We fill what we can fill now: shard_id, location_in_shard and record_id.
        let record_iter = record_iter.map(|(idx, record)| {
            let mut loc = LocationBuilder::default();
            loc.set_shard_id(shard_id);
            loc.set_loc_in_shard(idx);
            loc.set_record_id(record.warc_id().to_string());

            (loc, record)
        });

        // identify
        let record_iter = record_iter
            .map(|(loc, record)| (loc, Self::process_record(record, identifier)))
            .filter_map(|(loc, res)| match res {
                Ok(Some(res)) => Some((loc, res)),
                Ok(None) => None,
                Err(e) => {
                    error!("{:?}", e);
                    None
                }
            });

        // remove short lines
        let length_filter = transformers::RemoveShortSentences::default();
        // let record_iter = record_iter.map(|(idx, r)| (idx, length_filter.transform_own(r)));

        // We get bounds of the most significant part.
        // transform_idx yields a vector of ranges, but we'll assume
        // there's one and only one, discarding if there's none,
        // and taking the first one + yielding an error if there's more.
        let record_iter = record_iter.filter_map(|(mut loc, record)| {
            let (record, bounds) = length_filter.transform_idx(record);
            match bounds.len() {
                0 => {
                    error!("record {} has no sentences kept", record.warc_id());
                    None
                }
                1 => {
                    loc.set_line_start(*bounds[0].start());
                    loc.set_line_end(*bounds[0].end());
                    Some((loc, record))
                }
                _ => {
                    warn!(
                        "record {} has more than one chunk of sentences kept",
                        record.warc_id()
                    );
                    loc.set_line_start(*bounds[0].start());
                    loc.set_line_end(*bounds[0].end());
                    Some((loc, record))
                }
            }
        });

        // annotate
        let adult_filter = transformers::ContentDetector::with_defaults()?;
        let record_iter =
            record_iter.map(|(loc, r)| (adult_filter.transform_own(r), loc.build().unwrap()));

        Ok((shard_id, record_iter.collect()))
    }

    /// process a record
    /// identify each line of the document
    /// then compute the most present identification
    fn process_record(
        record: Record<BufferedBody>,
        identifier: &identifiers::FastText,
    ) -> Result<Option<Document>, Error> {
        // get lines
        let (headers, body) = record.into_raw_parts();
        let body = String::from_utf8_lossy(&body);
        let lines = body.lines();

        // per-lang and total byte counts
        let mut lang_count = HashMap::new();
        let mut total_count = 0;

        // filter out unicode null chars
        // this prevents fasttext errors and hopefully improves
        // corpus quality
        let lines = lines.map(|l| l.replace(char::from(0), ""));

        // get identifications
        // We use option because of sentences that can't be properly identified
        let ids: Vec<Option<Identification>> = lines
            .map(|line| {
                // identify
                let id = identifier.identify(&line);

                // add to byte count for document-level identification
                if let Ok(Some(ref ide)) = id {
                    let byte_count = line.bytes().count();
                    lang_count
                        .entry(*ide.label())
                        .and_modify(|count| *count += byte_count)
                        .or_insert(byte_count);

                    total_count += byte_count;
                }

                id
            })
            .collect::<Result<_, Error>>()?;

        // figure out document language
        // count bytes per language, get language that got most bytes
        let document_language = lang_count.iter().max_by_key(|(_, v)| *v);

        if let Some((id, lang_byte_count)) = document_language {
            // build an Identification with prob = number of bytes from most identified language / total number of bytes
            let document_identification =
                Identification::new(*id, *lang_byte_count as f32 / total_count as f32);

            let metadata = Metadata::new(&document_identification, &ids);
            let doc = Document::new(body.into_owned(), headers.headers, metadata);

            //TODO: create location data
            debug!("{} : {:?}", doc.warc_id(), doc.identification());
            Ok(Some(doc))
        } else {
            debug!(
                "{:?} : NONE",
                headers
                    .headers
                    .get(&WarcHeader::RecordID)
                    .map(|x| Some(String::from_utf8_lossy(x)))
            );
            Ok(None)
        }
    }

    /// Gets a vector of documents and outputs a hashmap listing the documents per language
    fn sort_by_lang(
        documents: Vec<(Document, Location)>,
    ) -> HashMap<Lang, Vec<(Document, Location)>> {
        let mut ret = HashMap::new();
        for (document, location) in documents {
            let e = ret
                .entry(*document.identification().label())
                .or_insert_with(Vec::new);
            e.push((document, location));
        }

        ret
    }

    /// concurrently write documets
    fn write_documents<'a>(
        langfiles: &LangFilesDoc,
        avrowriters: &'a RebuildWriters<'a, File>,
        shard_id: usize,
        documents: HashMap<Lang, Vec<(Document, Location)>>,
    ) -> Result<(), Error> {
        let errors: Vec<Error> = documents
            .into_par_iter()
            .map(|(lang, docs)| {
                debug!("[{}]: {} documents", lang, docs.len());

                // get mutexes on writers
                let writer = langfiles.writers().get(&lang).unwrap();
                let avrowriter = avrowriters.get(&lang).unwrap();
                let mut writer_lock = writer.lock().unwrap();
                let mut avrowriter_lock = avrowriter.lock().unwrap();

                // divide the documents iterator into two iterators
                let (docs, locations): (Vec<_>, Vec<_>) =
                    docs.into_iter().map(|(doc, loc)| (doc, loc)).unzip();

                // clone metadata
                let metadata_cloned = docs.iter().map(|doc| doc.metadata().clone()).collect();
                let sr = ShardResult::new(shard_id as i64, locations, metadata_cloned);

                // write docs and rebuild files
                writer_lock.write(docs)?;
                avrowriter_lock.append_ser(sr)?;

                //TODO: not sure that we need the flush
                avrowriter_lock.flush()?;

                Ok(())
            })
            // only collect errors
            .filter_map(|x| match x {
                Ok(_) => None,
                Err(e) => Some(e),
            })
            .collect();

        for error in errors {
            error!("{:?}", error);
        }

        Ok(())
    }
}

impl Pipeline<()> for OscarDoc {
    fn version() -> &'static str {
        "2.0.0"
    }

    fn run(&self) -> Result<(), Error> {
        // let errors;

        let cls = FastText::new(&self.lid_path, 1, 0.8)?;

        let results = self.get_paths_iter()?;

        // convert to parallel iterator
        // /!\: We use par_bridge, that is suboptimal
        //      compared to implementing IntoParallelIterator
        //      ourselves.
        let results = results.enumerate().par_bridge();

        let langfiles = LangFilesDoc::new(&self.dst, None)?;
        let mut dst_rebuild = self.dst.clone();
        dst_rebuild.push("rebuild");

        let rebuild_files = RebuildWriters::with_dst(&dst_rebuild)?;

        //iterate over shards
        let shards_results =
            results.map(|(idx, shard)| (idx, Self::process_shard(&shard, &cls, None)));

        // for each shard result, sort by lang and write concurrently.
        shards_results.for_each(|(idx, shard_result)| {
            if let Ok((shard_id, shard_result)) = shard_result {
                let hm = Self::sort_by_lang(shard_result);
                // println!("{:#?}", hm.get(&Lang::Fr));
                // // TODO write rebuild
                // let hm = hm
                //     .into_iter()
                //     .map(|(k, v)| (k, v.into_iter().map(|(doc, _)| doc).collect()))
                //     .collect();
                Self::write_documents(&langfiles, &rebuild_files, shard_id, hm).unwrap();
            } else {
                error!("Error with shard idx {}:{:?}", idx, shard_result);
            }
        });

        Ok(())
    }
}
