/*! Corpus rebuilding.

 Corpus rebuilding is the action of taking `rebuild files` and `shards` and "merge" them to recreate the corpus.

 This module contains mainly iterators that make rebuilding easier to do and parallelize.

 * [RecordIterator] iteratively returns [Document]s from a **single** avro record (which corresponds to a **single** shard).
 * [SRIterator] iteratively returns [RecordIterator]s from a **single** avro file (which corresponds to several shards).
 * [todo] calls [Iterator::next] on [SRIterator] and uses `n` threads to retrieve [Document]s and do IO to recreate the corpus.
* !*/
use crate::io::writer::WriterDoc;
use crate::io::writer::WriterTrait;
use crate::pipelines::oscardoc::types::Document;
use crate::pipelines::oscardoc::types::RebuildInformation;
use crate::pipelines::oscardoc::types::ShardResult;
use crate::sources::commoncrawl::Wet;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::vec::IntoIter;

use flate2::read::MultiGzDecoder;
use itertools::Itertools;
use log::debug;
use log::error;
use rayon::iter::ParallelBridge;
use rayon::iter::ParallelIterator;
use warc::RecordIter;

use crate::error::Error;
use crate::lang::Lang;

/// Iterator over reconstitued documents from a rebuild file, for a single shard and a single language.
///
/// Propagates errors from warc, and stops iterating if there's a record_id mismatch between rebuild file and shard data.
pub struct RecordIterator<T, I>
where
    T: BufRead,
    I: Iterator<Item = RebuildInformation>,
{
    rebuild_iter: I,
    shard_iter: RecordIter<T>,
    shard_id: usize,

    prev_loc: usize,
}

impl<T, I> RecordIterator<T, I>
where
    T: BufRead,
    I: Iterator<Item = RebuildInformation>,
{
    fn new(rebuild_iter: I, shard_iter: RecordIter<T>, shard_id: usize) -> Self {
        debug!("opening iterator on shard {}", shard_id);
        Self {
            rebuild_iter,
            shard_iter,
            shard_id,
            prev_loc: 0,
        }
    }

    /// Get a reference to the record iterator's shard id.
    pub fn shard_id(&self) -> usize {
        self.shard_id
    }
}

impl<T, I> Iterator for RecordIterator<T, I>
where
    T: BufRead,
    I: Iterator<Item = RebuildInformation>,
{
    type Item = Result<Document, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(rb_info) = self.rebuild_iter.next() {
            // get loc of current rebuild
            let loc = rb_info.loc_in_shard();
            let rid = rb_info.record_id();

            // We skip loc-prev_loc records (since we have absolute loc counts, we need to compute the delta)
            if loc < self.prev_loc {
                // technically we could "go back" using the bufreader and rewinding.
                // TODO: implement this? We could also go from line-based to byte-based offset
                // to enable faster retrieval.
                error!("It looks like the rebuild file is not ordered. Rebuilding can't work from there, aborting.");
                return None;
            }
            let record = match self.shard_iter.nth(loc - self.prev_loc) {
                Some(Ok(r)) => r,
                //uj: should we really "just" return some error or return None (with error logging)
                Some(Err(e)) => return Some(Err(e.into())),
                None => return None,
            };

            // ensure that we got the right record
            if record.warc_id() != rid {
                error!(
                    "record_id mismatch! shard number {}: shard: {}, rebuild {}",
                    rb_info.shard_id(),
                    record.warc_id(),
                    rid
                );
                // return error?
                return None;
            }

            // separate raw parts
            let (headers, body) = record.into_raw_parts();

            // compute line bounds and get them
            let nb_skip = rb_info.line_start();

            // Since bounds are inclusive, for a document that starts at x and ends at y we have to skip to x
            // and then take y-x+1.
            let nb_take = rb_info.line_end() - rb_info.line_start() + 1;
            let body = String::from_utf8_lossy(&body)
                .lines()
                .skip(nb_skip)
                .take(nb_take)
                .join("\n");

            // create document and update prev_loc
            let document = Document::new(body, headers.headers, rb_info.metadata().clone());
            self.prev_loc = loc + 1;

            Some(Ok(document))
        } else {
            None
        }
    }
}

/// Iterator that yields a [RecordIterator] for each entry in the avro file.
///
/// When calling [Iterator::next], an avro record and a shard are read and a [RecordIterator] is built on them.
pub struct SRIterator<'a> {
    src_shards: &'a Path,
    rebuild_reader: avro_rs::Reader<'a, BufReader<File>>,
}

impl<'a> SRIterator<'a> {
    pub fn new(src_rebuild: &'a Path, src_shards: &'a Path) -> Result<Self, Error> {
        //check validity of provided files/folders
        if src_rebuild.is_dir() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "src_rebuild must be pointing to an avro file! (is {:?})",
                    src_rebuild
                ),
            )));
        }
        if !src_shards.is_dir() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "src_shards must be pointing to a folder! (is {:?})",
                    src_rebuild
                ),
            )));
        }

        // open avro reader
        let f = File::open(src_rebuild)?;
        let f = BufReader::new(f);
        let rebuild_reader = avro_rs::Reader::new(f)?;

        Ok(Self {
            src_shards,
            rebuild_reader,
        })
    }
}

impl<'a> Iterator for SRIterator<'a> {
    type Item = RecordIterator<BufReader<MultiGzDecoder<File>>, IntoIter<RebuildInformation>>;

    fn next(&mut self) -> Option<Self::Item> {
        // get next entry in avro file
        let next_rebuild = match self.rebuild_reader.next() {
            Some(Ok(nr)) => nr,
            None => return None,
            Some(Err(e)) => {
                error!("{}", e);
                return None;
            }
        };

        // deserialize entry into a shard result
        let shard_result: ShardResult = match avro_rs::from_value(&next_rebuild) {
            Ok(sr) => sr,
            Err(e) => {
                error!("{}", e);
                return None;
            }
        };

        debug!(
            "shard {}: {} records to rebuild",
            shard_result.shard_id(),
            shard_result.rebuild_info().len()
        );

        //TODO remove as keyword
        let shard_id = shard_result.shard_id() as usize;

        // forge shard path
        let mut shard_path = PathBuf::from(self.src_shards);
        shard_path.push(format!("{}.txt.gz", shard_id));

        //open shard, get iterator and build RecordIterator
        //TODO: yield Results
        let shard_iter = Wet::from_path_gzip(shard_path).unwrap().iter;
        let (_, rebuild_info) = shard_result.into_raw_parts();
        let rebuild_iter = rebuild_info.into_iter();
        Some(RecordIterator::new(rebuild_iter, shard_iter, shard_id))
    }
}

/// Corpus rebuilder for a single language.
pub struct Rebuilder<'a> {
    src_rebuild: &'a Path,
    src_shards: &'a Path,
    dst: &'a Path,
    lang: Lang,
}

impl<'a> Rebuilder<'a> {
    pub fn new(src_rebuild: &'a Path, src_shards: &'a Path, dst: &'a Path, lang: Lang) -> Self {
        Self {
            src_rebuild,
            src_shards,
            dst,
            lang,
        }
    }

    /// Reads the rebuild file, then opens each specified shard and extracts relevant records.
    pub fn run(self) -> Result<(), Error> {
        // Get iterator over rebuild
        // in parallel
        let sr = SRIterator::new(self.src_rebuild, self.src_shards)?;
        let sr = sr.par_bridge();

        // create mutex
        let wr = Arc::new(Mutex::new(WriterDoc::new(
            self.dst,
            self.lang.to_static(),
            None,
        )?));

        // iterate over shard results
        let errors: Vec<Result<(), Error>> = sr
            .map(|shard| {
                let shard_id = shard.shard_id();
                debug!("working on shard {shard_id}");
                // get records of a given shard
                let records: Vec<_> = shard.collect::<Result<Vec<Document>, Error>>()?;

                // attempt to write
                let mut wr_locked = wr.lock().unwrap();
                debug!("[{}] writing {} results to disk", shard_id, records.len());
                wr_locked.write(records)?;
                debug!("[{}] done", shard_id);
                Ok(())
            })
            .collect();

        // print out eventual errors
        for error in errors.iter().filter(|x| x.is_err()) {
            error!("{:?}", error);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        io::{BufReader, Cursor},
    };

    use oxilangtag::LanguageTag;
    use warc::WarcReader;

    use crate::{
        identifiers::identification::Identification,
        pipelines::oscardoc::types::{Document, Metadata},
    };

    fn test_from_loc_meta() {
        let raw = b"\
            WARC/1.0\r\n\
            Warc-Type: dunno\r\n\
            Content-Length: 5\r\n\
            WARC-Record-Id: <urn:test:two-records:record-0>\r\n\
            WARC-Date: 2020-07-08T02:52:55Z\r\n\
            \r\n\
            123455\r\n\
            \r\n\
            WARC/1.0\r\n\
            Warc-Type: another\r\n\
            WARC-Record-Id: <urn:test:two-records:record-1>\r\n\
            WARC-Date: 2020-07-08T02:52:56Z\r\n\
            Content-Length: 6\r\n\
            \r\n\
            123456\r\n\
            \r\n\
        ";

        let shard_reader = BufReader::new(Cursor::new(raw));
        let shard_reader = WarcReader::new(shard_reader).iter_records();
        for s in shard_reader {
            println!("{:?}", s);
        }
        let content = String::from(
            "foo
        bar
        baz
        quux",
        );
        let warc_headers = HashMap::new();
        let metadata = Metadata::new(
            &Identification::new(LanguageTag::parse("en".to_string()).unwrap(), 1.0),
            &vec![
                Some(Identification::new(
                    LanguageTag::parse("en".to_string()).unwrap(),
                    1.0
                ));
                4
            ],
        );

        let _ = Document::new(content, warc_headers, metadata);
    }
}
