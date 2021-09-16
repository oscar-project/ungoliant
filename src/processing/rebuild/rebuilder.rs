/*!

# Rebuilder for <1.2 OSCAR Schema

This will generate index files that will make possible the rebuilding of OSCAR <1.2 which doesn't have the origin metadata enabling fast retreival.

The process is, for a given language:

- Read language corpus
- Store all record_ids ([build_record_index])
- Get a mapping of record_ids to shard_number ([link_records_to_shards]) (TODO: add a `position` field in order to ease seeking later )
- Get a mapping of shard_number -> Vec<record_ids> ([to_shards_to_records])
- reset language corpus
- for each (pertinent) shard
    -
// - for each shard
//     - filter records that are here (it should be sorted)
//     - get line numbers (in record from shard) of first and last sentences (in record from corpus)
//     - Build new Origin from record_id, line numbers, shard_number.
!*/

use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    fs::File,
    path::{Path, PathBuf},
};

use super::avro_schema::{SCHEMA_RECORD, SCHEMA_RECORD_LIST, SCHEMA_WHOLE};
use crate::{
    error::Error,
    io::reader::{
        reader::{PieceMeta, Reader},
        Corpus,
    },
    sources::commoncrawl::Wet,
};
use avro_rs::{Codec, Schema, Writer};
use log::debug;
use log::error;
use log::warn;

use super::location::Both as BothLocation;
use super::location::Corpus as CorpusLocation;
use crate::io::reader::ReaderTrait;

/// prepare a rebuild file for <1.2 Oscar schema
pub fn prep_rebuild(src_corpus: &Path, src_shards: &Path, dst: &Path) -> Result<(), Error> {
    let mut corpus = Corpus::new_bytes(src_corpus);

    // only load english language
    let mut language_corpus = corpus
        .readers
        .get_mut("en")
        .ok_or_else(|| Error::UnknownLang("en".to_string()))?;

    //get record ids of english corpus
    let record_ids = record_index(&mut language_corpus)?;
    debug!("got {:#?} records", record_ids.len());
    let shard_ids = shard_index(record_ids, src_shards)?;
    debug!("got {:#?} shards", shard_ids.len());

    std::fs::create_dir(&dst)?;
    let mut path_rebuild = PathBuf::from(dst);
    path_rebuild.push("en.avro");

    debug!("writing to {:?}", &path_rebuild);
    let f = File::create(&path_rebuild)?;

    let schema = Schema::parse_list(&[SCHEMA_RECORD, SCHEMA_RECORD_LIST, SCHEMA_WHOLE]).unwrap();
    debug!("{:#?}", schema);
    let mut wtr = Writer::with_codec(&schema[2], &f, Codec::Snappy);

    let shard_ids: HashMap<String, _> = shard_ids
        .into_iter()
        .map(|(k, v)| (format!("{}", k), v))
        .collect();

    wtr.append_ser(shard_ids).unwrap();

    Ok(())
}

#[inline]
fn extract_record_id(record: &PieceMeta) -> String {
    record
        .headers
        .headers
        .get(&warc::WarcHeader::RecordID)
        .unwrap()
        .to_string()
}

/// Build the record index.
///
/// A record index is a [HashMap] mapping record IDs to [CorpusLocation], rassembling (line)offset, nb_sentences, and (byte)offset (loc) in corpus file.
fn record_index(language_reader: &mut Reader) -> Result<HashMap<String, CorpusLocation>, Error> {
    let mut ret = HashMap::new();
    let mut cur_record = language_reader.next();

    // iterate while there are records to get
    while cur_record.is_some() {
        // unwrap is safe since we tested for is_some().
        let r = cur_record.unwrap()?;
        let record_id = extract_record_id(&r);

        // new corpuslocation from record.
        // note that the loc is set to 0.
        let mut c = CorpusLocation::from(r);

        // watch for a valid location from reader
        let loc = match language_reader.pos() {
            Some(Ok(pos)) => pos,

            // inner error (during IO)
            Some(Err(e)) => {
                error!("Could not read position of record");
                return Err(e);
            }

            // This should be catch at compile time, since
            // it is an implementation problem.
            // TODO: refactor textreader to provide better guarantees
            None => {
                error!("unable to get position from this reader");
                return Err(Error::Custom("Wrong kind of reader".to_string()));
            }
        };

        // put found loc in corpuslocation, insert and advance iterator
        c.set_loc(loc);
        ret.insert(record_id, c);
        cur_record = language_reader.next();
    }

    Ok(ret)
}

/// extracts shard number from n.gz
///
/// fails if filename is malformed
fn parse_shard_number(path: &Path) -> Result<u64, Error> {
    let shard_number: Option<Result<u64, Error>> = path
        .file_stem()
        .and_then(|s| s.to_str())
        .and_then(|s| s.split_once('.'))
        .map(|s| s.0.parse().map_err(Error::ParseInt));

    match shard_number {
        Some(Err(e)) => return Err(e),
        None => {
            return Err(Error::Custom(format!(
                "No shard number. Malformed file name? {:?}",
                path
            )))
        }
        _ => (),
    }

    let shard_number = shard_number.unwrap().unwrap();
    Ok(shard_number)
}

fn shard_index(
    records: HashMap<String, CorpusLocation>,
    src_shards: &Path,
) -> Result<HashMap<u64, Vec<BothLocation>>, Error> {
    let mut ret = HashMap::new();
    // get shard paths
    let shards = std::fs::read_dir(&src_shards)?
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

    for shard_path in shards {
        let shard_number = parse_shard_number(&shard_path)?;
        match process_shard(&shard_path, shard_number, &records) {
            Ok(v) => {
                // insert returns old value if there was one.
                // this way we check for return value and
                // inform if the key was already present.
                if ret.insert(shard_number, v).is_some() {
                    warn!("got same shard ({}) twice!", shard_number);
                }
            }
            Err(e) => {
                error!("Could not process shard {}: {:?}", shard_number, e)
            }
        }
    }
    Ok(ret)
}

fn process_shard(
    shard_path: &Path,
    shard_number: u64,
    records: &HashMap<String, CorpusLocation>,
) -> Result<Vec<BothLocation>, Error> {
    let shard = Wet::from_path_gzip(&shard_path)?;
    let mut ret = Vec::new();
    for (shard_record_number, shard_record) in shard.iter.enumerate() {
        let shard_record = shard_record?;
        let shard_record_id = shard_record.warc_id();
        match records.get(shard_record_id) {
            Some(r) => {
                ret.push(r.add_shard_loc(shard_record_id, shard_number, shard_record_number));
            }
            None => (),
        }
    }

    Ok(ret)
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use warc::Record;

    use crate::processing::Metadata;

    use super::*;

    #[test]
    fn test_extract_record_id() {
        // get expected record id
        let record = Record::default();
        let expected_record_id = record.warc_id().to_string();

        // extract using extract_record_id
        let headers = record.into_raw_parts().0.headers;
        let piece_meta = PieceMeta {
            sentences: Vec::new(),
            identification: "en",
            headers: Metadata::try_from(headers).unwrap(),
        };
        let result_record_id = extract_record_id(&piece_meta);

        assert_eq!(expected_record_id, result_record_id);
    }

    #[test]
    fn test_to_shards_to_records() {
        // let mut index = HashMap::new();
        // index.insert("r1".to_string(), 0);
        // index.insert("r2".to_string(), 1);
        // index.insert("r3".to_string(), 3);
        // index.insert("r4".to_string(), 4);
        // index.insert("r5".to_string(), 1);
        // index.insert("r6".to_string(), 0);
        // index.insert("r7".to_string(), 2);
        // index.insert("r8".to_string(), 2);

        // let shard_index = shard_index(&index);
        // todo!();

        // for (shard_number, mut r_ids) in shard_index {
        //     // sort to have a stable vec layout
        //     r_ids.sort();

        //     match shard_number {
        //         0 => assert_eq!(r_ids, vec!["r1", "r6"]),
        //         1 => assert_eq!(r_ids, vec!["r2", "r5"]),
        //         2 => assert_eq!(r_ids, vec!["r7", "r8"]),
        //         3 => assert_eq!(r_ids, vec!["r3"]),
        //         4 => assert_eq!(r_ids, vec!["r4"]),
        //         _ => panic!("invalid shard number"),
        //     }
        // }
    }
}
