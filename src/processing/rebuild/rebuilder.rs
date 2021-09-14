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
    path::Path,
};

use crate::{
    error::Error,
    io::reader::{
        reader::{PieceMeta, Reader},
        Corpus,
    },
    sources::commoncrawl::Wet,
};

use log::debug;
use log::error;
use log::warn;

use super::location::Corpus as CorpusLocation;

type Records = HashSet<String>;

/// prepare a rebuild file for <1.2 Oscar schema
pub fn prep_rebuild(src_corpus: &Path, src_shards: &Path, dst: &Path) -> Result<(), Error> {
    let mut corpus = Corpus::new_bytes(src_corpus);

    // only load english language
    let mut language_corpus = corpus
        .readers
        .get_mut("en")
        .ok_or_else(|| Error::UnknownLang("en".to_string()))?;

    //get record ids of english corpus
    let record_ids = build_record_index(&mut language_corpus)?;
    debug!(
        "size is {} bytes for {} entries",
        record_ids.iter().fold(0, |acc, x| acc + x.len()),
        record_ids.len()
    );

    // get rid -> shard_id correspondence
    let index = link_records_to_shards(record_ids, src_shards)?;

    let shards_to_open: HashSet<&u64> = index.values().collect();
    debug!(
        "{} shards to open : {:?}",
        shards_to_open.len(),
        shards_to_open
    );

    let zozz = to_shards_to_records(&index);
    println!("{:?}", zozz.get(&0));
    Ok(())
}

#[inline]
fn extract_record_id(record: PieceMeta) -> String {
    println!("{:#?}", record);
    record
        .headers
        .headers
        .get(&warc::WarcHeader::RecordID)
        .unwrap()
        .to_string()
}

/// build a record_id set
fn build_record_index(language_reader: &mut Reader) -> Result<HashSet<String>, Error> {
    language_reader
        .map(|record| match record {
            Ok(r) => Ok(extract_record_id(r)),
            Err(e) => Err(e),
        })
        .collect()
}

// fn record_index(
//     language_reader: &mut ByteReader,
// ) -> Result<HashMap<String, CorpusLocation>, Error> {
//     language_reader
//         .map(|record| match record {
//             Ok(r) => {
//                 let record_id = extract_record_id(r);
//                 let mut c = CorpusLocation::from(r);
//                 let loc = language_reader.pos();
//                 Ok(extract_record_id(r));
//             }
//             Err(e) => Err(e),
//         })
//         .collect()
// }

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

/// transform (record_id -> shard_id) to (shzard_id -> [record_ids]).
fn to_shards_to_records(records_to_shards: &HashMap<String, u64>) -> HashMap<u64, Vec<String>> {
    let shards = records_to_shards.values();
    let mut ret: HashMap<u64, Vec<String>> = HashMap::with_capacity(shards.count());

    for (record_id, shard_id) in records_to_shards {
        ret.entry(*shard_id)
            .or_insert_with(Vec::new)
            .push(record_id.to_string());
    }

    ret
}

/// link record_id to shard_number
/// TODO: maybe put start/end search in this?
fn link_records_to_shards(
    records: Records,
    src_shards: &Path,
) -> Result<HashMap<String, u64>, Error> {
    // init with capacity since we know it beforehand.
    // this will save time because of limited/no reallocation
    let mut links: HashMap<String, u64> = HashMap::with_capacity(records.len());

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

    for r in shards {
        let shard_number = parse_shard_number(&r)?;

        // open shard
        let shard = Wet::from_path_gzip(r)?;
        debug!("working on shard {}", shard_number);

        // fetch record_ids
        let records_in_shard: HashSet<String> = shard
            .iter
            .filter_map(|r| match r {
                Ok(r) => Some(r.warc_id().to_string()),
                Err(e) => {
                    error!("error reading record: {}", e);
                    None
                }
            })
            .collect();

        for r in records.intersection(&records_in_shard) {
            links.insert(r.to_string(), shard_number);
        }
    }
    Ok(links)
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
        let result_record_id = extract_record_id(piece_meta);

        assert_eq!(expected_record_id, result_record_id);
    }

    #[test]
    fn test_to_shards_to_records() {
        let mut index = HashMap::new();
        index.insert("r1".to_string(), 0);
        index.insert("r2".to_string(), 1);
        index.insert("r3".to_string(), 3);
        index.insert("r4".to_string(), 4);
        index.insert("r5".to_string(), 1);
        index.insert("r6".to_string(), 0);
        index.insert("r7".to_string(), 2);
        index.insert("r8".to_string(), 2);

        let shard_index = to_shards_to_records(&index);

        for (shard_number, mut r_ids) in shard_index {
            // sort to have a stable vec layout
            r_ids.sort();

            match shard_number {
                0 => assert_eq!(r_ids, vec!["r1", "r6"]),
                1 => assert_eq!(r_ids, vec!["r2", "r5"]),
                2 => assert_eq!(r_ids, vec!["r7", "r8"]),
                3 => assert_eq!(r_ids, vec!["r3"]),
                4 => assert_eq!(r_ids, vec!["r4"]),
                _ => panic!("invalid shard number"),
            }
        }
    }
}
