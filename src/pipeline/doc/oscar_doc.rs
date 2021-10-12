use std::path::Path;
use std::{collections::HashMap, path::PathBuf};

use crate::error::Error;
use crate::filtering::{record, Filter};
use crate::identifiers::{self, Identification, Identifier};
use crate::io::writer::WriterTrait;
use crate::lang::{Lang, LANG};
use crate::pipeline::doc::document::{Document, Metadata};
use crate::sources::commoncrawl::Wet;
use crate::transformers;
use crate::{identifiers::FastText, processing::document::MergedPiece};
use fasttext::Prediction;
use log::Level::Debug;
use log::{debug, error, info, log_enabled, warn};
use rayon::prelude::*;
use std::convert::TryFrom;
use warc::BufferedBody;
use warc::{Record, WarcHeader};

use crate::io::{LangFiles, LangFilesDoc};
/// OSCAR v1.5 generation pipeline
///
/// OSCAR v1.5 is a retrocompatible corpus
/// enhanced with metadata coming from CommonCrawl.
///
/// The CommonCrawl dump is composed of shards,
/// Each shard is composed of records,
/// Each record is composed of a metadata header and a body containing sentences.
///
/// # Processing
/// _every scope is concurrent, that means green threads are created on shards, records and sentences._
/// - We process each record separately, getting a list of sentence-language pairs, along with metadata from the document.
/// - Once we've treated each record of a given shard, we
///   transform out list of sentence-language pairs into chunks of contiguous same-language sentences
///   and we store shard-level line offsets on metadata.
///   Then we group same-language chunks for each language (on shard-level) and we write on disk.
/// - We also keep track of disk-level line offsets to sync shard-level offsets between writes.
///
/// TODO: Better document this step.
pub struct OscarDoc {
    src: PathBuf,
    dst: PathBuf,
    lid_path: PathBuf,
}

// /// convinience type alias for [warc::Record] headers.
// type WarcHeaders = HashMap<WarcHeader, Vec<u8>>;

impl OscarDoc {
    pub fn new(src: PathBuf, dst: PathBuf, lid_path: PathBuf) -> Self {
        Self { src, dst, lid_path }
    }

    /// list files in source folder,
    /// filter out errors from fs and from gzip/wet.
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

    /// Process a shard
    fn process_shard(
        shard_path: &Path,
        identifier: &identifiers::FastText,
        filter: Option<record::FilterKind>,
    ) -> Result<Vec<Document>, Error> {
        info!("working on shard: {:?}", shard_path);
        let shard = Wet::from_path_gzip(&shard_path)?;
        let record_iter = shard.iter.par_bridge();

        // get specified filter or resort to default filter kind
        let f = filter.unwrap_or_else(record::FilterKind::default);

        // get iterator on filtered records.
        // only get records that are valid *and* pass the filter.
        let record_iter = record_iter.filter_map(|record| match record {
            Ok(r) => {
                if f.detect(&r) {
                    Some(r)
                } else {
                    None
                }
            }
            Err(e) => {
                error!("{:?}", e);
                None
            }
        });

        // identify
        let record_iter = record_iter
            .map(|record| Self::process_record(record, identifier))
            .filter_map(|res| match res {
                Ok(Some(res)) => Some(res),
                Ok(None) => None,
                Err(e) => {
                    // error!("{:?}", e);
                    None
                }
            });

        // annotate
        let adult_filter = transformers::ContentDetector::default();
        let record_iter: Vec<Document> =
            record_iter.map(|r| adult_filter.transform_own(r)).collect();

        // let mut adult_counter = 0;
        // let mut non_adult_counter = 0;
        // for document in record_iter.iter() {
        //     if document.metadata().annotation().is_some() {
        //     // if let Some(_) = document.metadata().annotation() {
        //         adult_counter += 1;
        //         info!(
        //             "[{}]detected {:#?}",
        //             document.identification().label(),
        //             String::from_utf8_lossy(
        //                 document.warc_headers().get(&WarcHeader::TargetURI).unwrap()
        //             )
        //         );
        //         // info!("{}", document.content());
        //     } else {
        //         non_adult_counter += 1;
        //     }
        // }
        // info!(
        //     "annotated {}/{}  as adult links",
        //     adult_counter, non_adult_counter
        // );
        Ok(record_iter)
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

        // get identifications
        // We use option because of sentences that can't be properly identified
        let ids: Vec<Option<Identification>> = lines
            .map(|line| {
                // identify
                let id = identifier.identify(line);

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
    fn sort_by_lang(documents: Vec<Document>) -> HashMap<Lang, Vec<Document>> {
        let mut ret = HashMap::new();
        for document in documents {
            let e = ret
                .entry(*document.identification().label())
                .or_insert_with(Vec::new);
            e.push(document);
        }

        ret
    }

    // concurrently write documets
    fn write_documents(
        langfiles: &LangFilesDoc,
        documents: HashMap<Lang, Vec<Document>>,
    ) -> Result<(), Error> {
        documents.into_par_iter().for_each(|(lang, docs)| {
            debug!("[{}]: {} documents", lang, docs.len());
            let writer = langfiles.writers().get(&lang).unwrap();
            let mut writer_lock = writer.lock().unwrap();
            writer_lock.write(docs).unwrap();
        });

        Ok(())
    }

    pub fn run(&self) -> Result<(), Error> {
        // let errors;

        let cls = FastText::new(&self.lid_path, 1, 0.8)?;

        let results = self.get_paths_iter()?;

        // convert to parallel iterator
        // /!\: We use par_bridge, that is suboptimal
        //      compared to implementing IntoParallelIterator
        //      ourselves.
        let results = results.enumerate().par_bridge();

        let langfiles = LangFilesDoc::new(&self.dst, None)?;

        //iterate over shards
        let shards_results =
            results.map(|(idx, shard)| (idx, Self::process_shard(&shard, &cls, None)));

        // for each shard result, sort by lang and write concurrently.
        shards_results.for_each(|(idx, shard_result)| {
            if let Ok(shard_result) = shard_result {
                let hm = Self::sort_by_lang(shard_result);
                Self::write_documents(&langfiles, hm).unwrap();
            } else {
                error!("Error with shard idx {}:{:?}", idx, shard_result);
            }
        });
        Ok(())
    }
}