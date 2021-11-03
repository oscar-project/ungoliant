use crate::pipelines::oscardoc::types::Document;
use crate::pipelines::oscardoc::types::RebuildInformation;
use crate::pipelines::oscardoc::types::ShardResult;
use crate::sources::commoncrawl::Wet;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::path::Path;
use std::path::PathBuf;
use std::slice::Iter;
use std::vec::IntoIter;

use avro_rs::Reader;
use flate2::read::MultiGzDecoder;
use itertools::Itertools;
use log::error;
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

    prev_loc: usize,
}

impl<T, I> RecordIterator<T, I>
where
    T: BufRead,
    I: Iterator<Item = RebuildInformation>,
{
    fn new(rebuild_iter: I, shard_iter: RecordIter<T>) -> Self {
        Self {
            rebuild_iter,
            shard_iter,
            prev_loc: 0,
        }
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
            // let mut record = self.shard_iter.nth(loc - self.prev_loc).unwrap();
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
            let nb_take = rb_info.line_end() - rb_info.line_start();
            let body = String::from_utf8_lossy(&body)
                .lines()
                .skip(nb_skip)
                .take(nb_take)
                .join("\n");

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
            None | Some(Err(_)) => return None,
        };

        // deserialize entry into a shard result
        let shard_result: ShardResult = match avro_rs::from_value(&next_rebuild) {
            Ok(sr) => sr,
            Err(e) => return None,
        };

        let shard_id = shard_result.shard_id();
        //open shard
        //TODO: yield Results
        let shard_iter = Wet::from_path_gzip(self.src_shards).unwrap().iter;
        let (_, rebuild_info) = shard_result.into_raw_parts();
        let rebuild_iter = rebuild_info.into_iter();

        Some(RecordIterator::new(rebuild_iter, shard_iter))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        fs::File,
        io::{BufReader, Cursor},
        path::Path,
    };

    use itertools::Itertools;
    use warc::WarcReader;

    use crate::{
        filtering::record,
        identifiers::Identification,
        lang::Lang,
        pipelines::oscardoc::types::{Document, Metadata, ShardResult},
        sources::commoncrawl::Wet,
    };

    #[test]
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
            &Identification::new(Lang::En, 1.0),
            &vec![Some(Identification::new(Lang::En, 1.0)).clone(); 4],
        );

        let doc = Document::new(content, warc_headers, metadata);
    }

    #[test]
    fn test_deser() {
        let src = Path::new("../cc/processed/rebuild/fr.avro");
        let f = File::open(src).unwrap();

        let ar = avro_rs::Reader::new(f).unwrap();

        // get first shard result
        let avro_val = ar.into_iter().next().unwrap().unwrap();
        let sr: ShardResult = avro_rs::from_value(&avro_val).unwrap();

        let mut sr_iter = sr.rebuild_info().into_iter();

        println!("{:#?}", sr.shard_id());

        //open shard
        let shard_path = Path::new("../cc/shards/6.txt.gz");
        let shard = Wet::from_path_gzip(shard_path).unwrap();

        //TODO: use streaming_iter to improve performance
        let mut shard_iter = shard.iter;

        // location of previous record in rebuild
        let mut prev_loc = 0;
        //iterate on rebuild
        while let Some(rb_info) = sr_iter.next() {
            // get loc of current rebuild
            let loc = rb_info.loc_in_shard();
            let rid = rb_info.record_id();
            // We skip loc-prev_loc records (since we have absolute loc counts, we need to compute the delta)
            let mut record = shard_iter.nth(loc - prev_loc).unwrap().unwrap();

            // ensure that we got the right record
            assert_eq!(record.warc_id(), rid);

            // separate raw parts
            let (headers, body) = record.into_raw_parts();

            // compute line bounds and get them
            let nb_skip = rb_info.line_start();
            let nb_take = rb_info.line_end() - rb_info.line_start();
            let body = String::from_utf8_lossy(&body)
                .lines()
                .skip(nb_skip)
                .take(nb_take)
                .join("\n");

            let document = Document::new(body, headers.headers, rb_info.metadata().clone());
            prev_loc = loc + 1;
        }

        for rb_info in sr.rebuild_info() {
            println!("{:#?}", rb_info.loc_in_shard());
        }
    }

    // fn test_ser() {
    //     let meta = vec![Metadata::default()];
    //     let loc = vec![Location::default()];
    //     let sr = ShardResult::new(0, loc, meta);
    //     println!("{:#?}", sr);
    //     println!("{:#?}", *super::SCHEMA);
    //     let mut buf = Vec::new();
    //     let mut rw = RebuildWriter::new(&super::SCHEMA, &mut buf);

    //     rw.append_ser(&sr).unwrap();
    //     rw.flush().unwrap();

    //     let ar = avro_rs::Reader::with_schema(&super::SCHEMA, &buf[..]).unwrap();
    //     let result: Vec<ShardResult> = ar
    //         .map(|r| avro_rs::from_value::<ShardResult>(&r.unwrap()).unwrap())
    //         .collect();
    //     assert_eq!(result.len(), 1);
    //     assert_eq!(result[0], sr);
    // }
}
