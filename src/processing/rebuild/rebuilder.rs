/*!

# Rebuilder for <1.2 OSCAR Schema

This will generate index files that will make possible the rebuilding of OSCAR <1.2 which doesn't have the origin metadata enabling fast retreival.

The process is, for a given language:

- Read language corpus
- Store all record_ids
- for each shard
    - filter records that are here
    - get line numbers (in record from shard) of first and last sentences (in record from corpus)
    - Build new Origin from record_id, line numbers, shard_number.
!*/

use std::{
    collections::{HashMap, HashSet},
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

type Records = HashSet<String>;

/// prepare a rebuild file for <1.2 Oscar schema
pub fn prep_rebuild(src_corpus: &Path, src_shards: &Path, dst: &Path) -> Result<(), Error> {
    let mut corpus = Corpus::new(src_corpus);

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

    link_records_to_shards(record_ids, src_shards);
    Ok(())
}

/// build a record_id set
fn build_record_index(language_reader: &mut Reader) -> Result<Records, Error> {
    #[inline]
    fn extract_record_id(record: PieceMeta) -> String {
        record
            .headers
            .headers
            .get(&warc::WarcHeader::RecordID)
            .unwrap()
            .to_string()
    }

    language_reader
        .map(|record| match record {
            Ok(r) => Ok(extract_record_id(r)),
            Err(e) => Err(e),
        })
        .collect()
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
        // go from "9999.txt.gz" to 9999
        // There has to be a simpler way.
        let shard_number: Option<Result<u64, Error>> = r
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.split_once('.'))
            .map(|s| s.0.parse().map_err(Error::ParseInt));
        match shard_number {
            Some(Err(e)) => return Err(e),
            None => {
                return Err(Error::Custom(format!(
                    "No shard number. Malformed file name? {:?}",
                    r
                )))
            }
            _ => (),
        }

        let shard_number = shard_number.unwrap().unwrap();

        // open shard
        let shard = Wet::from_path_gzip(r)?;

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
