use std::path::Path;
use std::{collections::HashMap, path::PathBuf};

use crate::error::Error;
use crate::filtering::{record, Filter};
use crate::identifiers::{self, Identification, Identifier};
use crate::lang::LANG;
use crate::pipeline::doc::document::{Document, Metadata};
use crate::sources::commoncrawl::Wet;
use crate::{identifiers::FastText, processing::document::MergedPiece};
use fasttext::Prediction;
use log::Level::Debug;
use log::{debug, error, info, log_enabled, warn};
use rayon::prelude::*;
use std::convert::TryFrom;
use warc::BufferedBody;
use warc::{Record, WarcHeader};

use crate::io::LangFiles;
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
        let r = record_iter
            .map(|record| Self::process_record(record, identifier))
            .filter_map(|res| match res {
                Ok(Some(res)) => Some(res),
                Ok(None) => None,
                Err(e) => {
                    // error!("{:?}", e);
                    None
                }
            });

        Ok(r.collect())
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

    pub fn run(&self) -> Result<(), Error> {
        // let errors;

        let cls = FastText::new(&self.lid_path, 1, 0.8)?;

        let results = self.get_paths_iter()?;

        // convert to parallel iterator
        // /!\: We use par_bridge, that is suboptimal
        //      compared to implementing IntoParallelIterator
        //      ourselves.
        let results = results.enumerate().par_bridge();

        let langfiles = LangFiles::new(&self.dst, None)?;

        //iterate over shards
        let r = results.map(|(idx, shard)| (idx, Self::process_shard(&shard, &cls, None)));
        r.for_each(|(idx, shard_result)| match shard_result {
            Ok(sr) => info!("found {} docs in shard {}", sr.len(), idx),
            Err(e) => error!("Error in shard {}: {:?}", idx, e),
        });
        Ok(())
    }
    //     /// attempt to predict language on provided sentence.
    //     ///
    //     /// Returns [None] if no language is detected.
    //     // why return the sentence itself?
    //     // TODO: change return type to Option<&'static str>.
    //     // fn identify_sentence(sentence: &str, cls: &FastText) -> Option<(String, &'static str)> {
    //     //     let prediction = cls.predict(sentence).ok();

    //     //     if let Some(Some(lang)) = prediction {
    //     //         //TODO: rewrite these two lines more elegantly
    //     //         //      we can unwrap since predict returns None if no predictions are
    //     //         //      found
    //     //         let lang = lang.get(0).unwrap();

    //     //         // check if fasttext provided lang exists
    //     //         // return None if not
    //     //         match LANG.get(lang.label.as_str()) {
    //     //             Some(lang) => Some((sentence.to_string(), *lang)),
    //     //             None => {
    //     //                 warn!("lang {} does not exist!", lang.label);
    //     //                 None
    //     //             }
    //     //         }
    //     //     } else {
    //     //         None
    //     //     }
    //     // }

    //     fn identify_sentence(sentence: &str, cls: &FastText) -> Option<&'static str> {
    //         let prediction = cls.predict(sentence).ok();

    //         if let Some(Some(lang)) = prediction {
    //             //TODO: rewrite these two lines more elegantly
    //             //      we can unwrap since predict returns None if no predictions are
    //             //      found
    //             let lang = lang.get(0).unwrap();

    //             // check if fasttext provided lang exists
    //             // return None if not
    //             match LANG.get(lang.label.as_str()) {
    //                 Some(lang) => Some(*lang),
    //                 None => {
    //                     warn!("lang {} does not exist!", lang.label);
    //                     None
    //                 }
    //             }
    //         } else {
    //             None
    //         }
    //     }

    //     /// Process a provided record.
    //     ///
    //     /// Here, sentences that are >100 chars are processed,
    //     /// and the others are discarded.
    //     /// See [String::chars::count].
    //     ///
    //     /// Then, we identify language for each sentence
    //     /// and return (sentence, language) along with headers
    //     /// extracted from the WARC.
    //     fn process_record(
    //         record: &Record<BufferedBody>,
    //         cls: &FastText,
    //     ) -> Option<(Vec<(String, &'static str)>, WarcHeaders)> {
    //         if log_enabled!(Debug) {
    //             debug!("processing record {}", record.warc_id());
    //         };
    //         let body = String::from_utf8(record.body().to_vec()).ok();

    //         // process record if body is utf8-valid
    //         if let Some(sentences) = body {
    //             // filter out lines that does not contain 100 characters.
    //             // then convert into a parallel iterator
    //             let sentences = sentences.lines().par_bridge();

    //             let results: Vec<&'static str> = sentences
    //                 // predict for each sentence, discarding
    //                 // predictions that does not meet threshold
    //                 .filter_map(|sentence| Self::identify_sentence(sentence, cls))
    //                 .collect();

    //             Some((results, record.into_raw_parts().0.headers))
    //         } else {
    //             error!("body not UTF-8 valid: {:?}", record.warc_id());
    //             None
    //         }
    //     }

    //     /// Run the whole pipeline
    // //     pub fn run(&self) -> Result<(), Error> {
    // //         // let errors;

    // //         let cls = FastText::new(&self.lid_path, 1, 0.8)?;

    // //         // list files in source folder,
    // //         // filter out errors from fs and from gzip/wet.
    // //         // This means that invalid gz files and invalid
    // //         // wet files are discarded silently
    // //         let results = std::fs::read_dir(&self.src)?
    // //             .filter_map(|shard| {
    // //                 shard.map_or_else(
    // //                     |e| {
    // //                         error!("error reading shard directory: {}", e);
    // //                         None
    // //                     },
    // //                     Some,
    // //                 )
    // //             })
    // //             .map(|shard| shard.path());

    // //         // convert to parallel iterator
    // //         // /!\: We use par_bridge, that is suboptimal
    // //         //      compared to implementing IntoParallelIterator
    // //         //      ourselves.
    // //         let results = results.enumerate().par_bridge();

    // //         // holds file handles
    // //         // let langfiles = match self.part_size {
    // //         //     Some(ps) => LangFiles::new(&self.dst, Some(ps * 1_000_000))?,
    // //         //     None => LangFiles::new(&self.dst, None)?,
    // //         // };

    // //         let langfiles = LangFiles::new(&self.dst, None)?;

    // //         // iterate over shards
    // //         let r: Vec<Error> = results
    // //             .filter_map(|(idx, shard)| {
    // //                 // holds merged pieces by lang
    // //                 let mut lang_pieces: HashMap<&'static str, Vec<MergedPiece>> = HashMap::new();

    // //                 // get an atomic reference to global offsets
    // //                 // let offsets_global_arc = offsets_global.clone();
    // //                 info!("processing shard {}: {:?}", idx, &shard);

    // //                 let shard = Wet::from_path_gzip(&shard);
    // //                 if shard.is_err() {
    // //                     error!("Could not read/open shard {}", idx);
    // //                     return shard.err();
    // //                 }

    // //                 let shard = shard.unwrap();
    // //                 // convert into a parallel iterator
    // //                 let wetfile = shard.iter.enumerate().par_bridge();

    // //                 let shard_results: Vec<(Vec<(String, &'static str)>, WarcHeaders)> = wetfile
    // //                     .filter_map(|(idx_record, record)| match record {
    // //                         Ok(record) => OscarDoc::process_record(&record, &cls),
    // //                         Err(e) => {
    // //                             warn!("Error on record {} of shard {}: {:?}", idx_record, idx, e);
    // //                             None
    // //                         }
    // //                     })
    // //                     // collect here is blocking
    // //                     // because we can't write concurrently into a HashMap
    // //                     // and using Mutexes might ruin performance.
    // //                     .collect(); //TODO: test with a for_each and a channel to send?

    // //                 // Iterate over (record, header) tuples
    // //                 let shard_results = shard_results.into_iter().filter_map(|(record, header)| {
    // //                     // split between langs and sentences
    // //                     let langs: Vec<&str> = record.iter().map(|(_, lang)| *lang).collect();
    // //                     let sentences: Vec<String> =
    // //                         record.into_iter().map(|(sentences, _)| sentences).collect();

    // //                     // create new document for current record
    // //                     let doc = Document::new(header, sentences, langs);

    // //                     match doc {
    // //                         Ok(doc) => Some(doc),
    // //                         Err(e) => {
    // //                             warn!("{:?}", e);
    // //                             None
    // //                         }
    // //                     }
    // //                 });

    // //                 // merge all documents together
    // //                 // get a vector of merged pieces of difference languages
    // //                 let docs_merged = shard_results
    // //                     .map(|doc| doc.into_merged_pieces_lang())
    // //                     .flatten()
    // //                     .collect::<Vec<MergedPiece>>();

    // //                 // sort merged pieces into different langs
    // //                 // now there's a hashmap that points each lang
    // //                 // to a vector of merged pieces
    // //                 for piece in docs_merged {
    // //                     let e = lang_pieces
    // //                         .entry(piece.identification())
    // //                         .or_insert_with(Vec::new);
    // //                     e.push(piece);
    // //                 }

    // //                 // write concurrently
    // //                 lang_pieces.into_par_iter().for_each(|(lang, pieces)| {
    // //                     let writer = langfiles.writers().get(lang).unwrap();
    // //                     let mut writer_lock = writer.lock().unwrap();
    // //                     writer_lock.write(pieces).unwrap();
    // //                 });

    // //                 None
    // //             })
    // //             .collect();

    // //         // fix trailing comma
    // //         // langfiles.close_meta()?;

    // //         for err in r {
    // //             error!("{:?}", err);
    // //         }

    // //         Ok(())
    // //     }
    // }

    // #[cfg(test)]
    // mod tests {
    //     use std::{env::temp_dir, path::PathBuf};

    //     use warc::{EmptyBody, Record};

    //     use crate::identifiers::FastText;

    //     use super::OscarDoc;

    //     #[test]
    //     fn test_process_record() {
    //         let cls = FastText::new_lid().unwrap();
    //         let record = ();

    //         // let oscar_metadata =
    //         //     OscarMetadata::new(temp_dir(), temp_dir(), PathBuf::from("lid.176.bin"));

    //         let mut record: Record<EmptyBody> = Record::default();
    //         let body = "english test that is longer than one hundred characters. english test that is longer than one hundred characters.
    // phrase française de plus de cent caractères. Ceci est une phrase française de plus de cent caractères.";
    //         println!("{}", body.len());
    //         let record = record.add_body(body);
    //         let identifications = OscarDoc::process_record(&record, &cls).unwrap();

    //         println!("{:#?}", identifications);
    //         assert!(false)
    //     }
}
