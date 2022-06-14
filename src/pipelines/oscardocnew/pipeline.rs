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
use std::str::Lines;
use std::{collections::HashMap, path::PathBuf};

use super::types::{Document, Location, Metadata, RebuildWriters};
use super::types::{LocationBuilder, ShardResult};
use crate::error::Error;
use crate::filtering::{record, Filter};
// use crate::identifiers::{self, Identification, Identifier};
// use crate::identifiers::{FastText, StrictMultilingual};
use crate::identifiers::identification::Identification;
use crate::identifiers::model::{FastText, New, Old, Predict};
use crate::io::writer::WriterTrait;
use crate::lang::Lang;
use crate::pipelines::pipeline::Pipeline;
use crate::sources::commoncrawl::Wet;
use crate::transformers::{
    self, Annotate, Annotator, ContentDetector, Header, Noisy, ShortSentences, TinyDocument,
    Transform,
};
use log::{debug, error, info, log_enabled, warn};
use oxilangtag::LanguageTag;
use rand::distributions::weighted;
use rayon::prelude::*;
use ut1_blocklist::Blocklist;
use warc::BufferedBody;
use warc::{Record, WarcHeader};

use crate::io::LangFilesDoc;

const DOC_THRESHOLD: f32 = 0.6f32;
pub struct OscarDoc {
    src: PathBuf,
    dst: PathBuf,
    lid_path: PathBuf,
    blocklist: Option<PathBuf>,
}

impl OscarDoc {
    pub fn new(src: PathBuf, dst: PathBuf, lid_path: PathBuf, blocklist: Option<PathBuf>) -> Self {
        if blocklist.is_none() {
            warn!("No blocklist folder specified! No adult content tagging will be done.");
        }

        debug!("using blocklist {:?}", blocklist);
        Self {
            src,
            dst,
            lid_path,
            blocklist,
        }
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
            .map(|s| s.parse::<usize>());

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
        identifier: &FastText<New>,
        filter: Option<record::FilterKind>,
        blocklist: &Option<PathBuf>,
    ) -> Result<(usize, Vec<(Document, Location)>), Error> {
        info!("working on shard: {:?}", shard_path);

        // get shard number
        let shard_id = Self::get_shard_number(shard_path)?;

        let shard = Wet::from_path_gzip(&shard_path)?;
        let record_iter = shard.iter.enumerate().par_bridge();

        // only get valid records, print errors
        let record_iter = record_iter.filter_map(|(idx, record)| match record {
            Ok(r) => Some((idx, r)),
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

        // remove short sentences, discarding documents that only have short sentences
        let length_filter = transformers::RemoveShortSentences::default();
        let record_iter = record_iter.filter_map(|(mut loc, mut record)| {
            let bounds = length_filter.transform(&mut record);
            match bounds.len() {
                0 => {
                    debug!("record {} has no sentences kept", record.warc_id());
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

        // get specified filter or resort to default filter kind
        let f = filter.unwrap_or_default();

        // get iterator on filtered records.
        // only get records that are valid *and* pass the filter.
        let record_iter = record_iter.filter_map(|(idx, record)| {
            if f.detect(&record) {
                Some((idx, record))
            } else {
                None
            }
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

        // annotate
        let mut annotator = Annotator::default();
        annotator
            .add(Box::new(TinyDocument::default()))
            .add(Box::new(ShortSentences::default()))
            .add(Box::new(Header::default()))
            .add(Box::new(Noisy::default()));

        if let Some(path) = blocklist {
            let bl = Blocklist::with_folder("adult", path)?;
            annotator.add(Box::new(ContentDetector::new(bl)));
        }

        let record_iter = record_iter.map(|(loc, mut r)| {
            annotator.annotate(&mut r);
            (r, loc.build().unwrap())
        });

        let record_iter = record_iter.filter_map(|(r, loc): (Document, Location)| {
            if r.metadata().annotation() == Some(&vec!["noisy".to_string(), "tiny".to_string()]) {
                debug!("removed document {:?} for noisy+tiny", r.warc_id());
                None
            } else {
                Some((r, loc))
            }
        });

        let records: Vec<(_, _)> = record_iter.collect();
        info!("Shard {}: Got {} documents", shard_id, records.len());

        Ok((shard_id, records))
    }

    /// process a record
    /// identify each line of the document
    /// then compute the most present identification
    fn process_record(
        record: Record<BufferedBody>,
        identifier: &FastText<New>,
    ) -> Result<Option<Document>, Error> {
        // get lines
        let (headers, body) = record.into_raw_parts();
        let body = String::from_utf8_lossy(&body);
        let lines = body.lines();

        // get the id for each line, the byte/prob count and the total byte count of the document
        // let (ids, lang_count, total_count) = identifier.weighted_ids(lines)?;
        let weighted_ids = identifier.weighted_ids(lines)?;

        // TODO: multilingual
        // see if the record meets multilingual criteria
        // let multilingual = StrictMultilingual::default().detect(weighted_ids.line_ids());

        // if multilingual {
        //     //TODO: fix prob on multilingual documents
        //     let document_identification = Identification::new(Lang::Multi, 0.5);

        //     let metadata = Metadata::new(&document_identification, &ids);
        //     let doc = Document::new(body.into_owned(), headers.headers, metadata);

        //     return Ok(Some(doc));
        // }

        // figure out document language
        // count bytes per language, get language that got most bytes
        let document_language = weighted_ids.lang_bins().iter().max_by_key(|(_, (v, _))| *v);

        // build a document and return it if the document language is not the unknown one.
        if let Some((Some(id), (lang_byte_count, confidence))) = document_language {
            // build an Identification with prob = number of bytes from most identified language / total number of bytes
            debug!(
                "{:?}: {}/{} (c:{})",
                id,
                lang_byte_count,
                weighted_ids.total_size(),
                confidence
            );

            if confidence < &DOC_THRESHOLD {
                return Ok(None);
            }

            // create id
            let document_identification = Identification::new(*id, *confidence);

            // create doc and metadata
            let metadata = Metadata::new(&document_identification, weighted_ids.line_ids());
            let doc = Document::new(body.into_owned(), headers.headers, metadata);

            debug!("{} : {:?}", doc.warc_id(), doc.identification());
            Ok(Some(doc))
        } else {
            if log_enabled!(log::Level::Debug) {
                debug!(
                    "{:?} : NONE",
                    headers
                        .headers
                        .get(&WarcHeader::RecordID)
                        .map(|x| Some(String::from_utf8_lossy(x)))
                );
                debug!("{:?}", weighted_ids.total_size());
                debug!("{}", &body);
            }
            Ok(None)
        }
    }

    /// Gets a vector of documents and outputs a hashmap listing the documents per language
    fn sort_by_lang(
        documents: Vec<(Document, Location)>,
    ) -> HashMap<LanguageTag<String>, Vec<(Document, Location)>> {
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

        let cls = FastText::new(&self.lid_path, 1, 0.8).expect(&format!(
            "Could not load language identifier at {:?}",
            self.lid_path
        ));

        if !self.dst.exists() {
            warn!("Destination file does not exist. Creating");
            std::fs::create_dir(&self.dst)?;
        }

        if !self.dst.is_dir() {
            panic!("Destination has to be a directory: {:?}", self.dst);
        }
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
        let shards_results = results.map(|(idx, shard)| {
            (
                idx,
                Self::process_shard(&shard, &cls, None, &self.blocklist),
            )
        });

        // for each shard result, sort by lang and write concurrently.
        shards_results.for_each(|(idx, shard_result)| {
            if let Ok((shard_id, shard_result)) = shard_result {
                let hm = Self::sort_by_lang(shard_result);
                Self::write_documents(&langfiles, &rebuild_files, shard_id, hm).unwrap();
            } else {
                error!("Error with shard idx {}:{:?}", idx, shard_result);
            }
        });

        Ok(())
    }
}
