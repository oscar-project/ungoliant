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
    collections::HashMap,
    fs::{self, File},
    hash::Hasher,
    io::BufRead,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use super::avro_schema::SCHEMA;
use super::shard_entry::{ShardEntry, ShardEntryAvro};
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
use log::{info, warn};
// use rayon::iter::{IntoParallelIterator, ParallelBridge, ParallelIterator};
use rayon::prelude::*;
use twox_hash::XxHash64;

use super::location::Both as BothLocation;
use super::location::Corpus as CorpusLocation;
use crate::io::reader::ReaderTrait;

/// prepare a rebuild file for <1.2 Oscar schema
pub fn prep_rebuild(src_corpus: &Path, src_shards: &Path, dst: &Path) -> Result<(), Error> {
    let mut corpus = Corpus::new_bytes(src_corpus);

    std::fs::create_dir(&dst)?;

    for (lang, mut reader) in corpus.readers {
        rebuild_lang(&mut reader, lang, src_shards, dst)?;
    }
    Ok(())
}

fn rebuild_lang(
    language_corpus: &mut Reader,
    lang: &'static str,
    src_shards: &Path,
    dst: &Path,
) -> Result<(), Error> {
    info!("prepping {} rebuild file", lang);
    let record_ids = record_index(language_corpus)?;
    info!("Got records");

    // create avro file
    let mut path_rebuild = PathBuf::from(dst);
    path_rebuild.push(format!("{}.avro", lang));

    info!("writing to {:?}", &path_rebuild);
    let f = File::create(&path_rebuild)?;

    //get shard paths
    let shard_paths = std::fs::read_dir(&src_shards)?
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

    // load schema and writer
    let schema = Schema::parse_str(SCHEMA)?;
    let wtr = Arc::new(Mutex::new(Writer::with_codec(&schema, &f, Codec::Snappy)));

    // iterate on shards
    shard_paths
        .collect::<Vec<PathBuf>>()
        .par_iter()
        .map(|shard_path| {
            debug!("[{}]indexing {:?}", lang, shard_path);
            let shard_ids = shard_index(&record_ids, &shard_path)?;
            let shard_ids: ShardEntryAvro = shard_ids.into();

            let wtr_arc = wtr.clone();
            let mut wtr_mutex = wtr_arc.lock().unwrap();
            wtr_mutex.append_ser(shard_ids)?;

            Ok(())
        })
        .collect::<Vec<Result<_, Error>>>();

    // for shard_path in shard_paths {
    //     debug!("indexing {:?}", shard_path);
    //     let shard_ids = shard_index(&record_ids, &shard_path)?;
    //     let shard_ids: ShardEntryAvro = shard_ids.into();
    //     wtr.append_ser(shard_ids)?;
    // }

    // open corpus and convert start_hash to start_line
    let mut path_rebuild_fixed = PathBuf::from(&path_rebuild);
    path_rebuild_fixed.set_file_name(format!("{}_lines.avro", lang));
    debug!("{:?}", path_rebuild_fixed);
    get_line_starts(&path_rebuild, src_shards, &path_rebuild_fixed)?;

    // delete old file, replace by new one with line offsets.
    fs::rename(path_rebuild_fixed, path_rebuild)?;
    Ok(())
}

/// Gets line starts in shard records.
fn get_line_starts(src_rebuild: &Path, src_shards: &Path, dst_rebuild: &Path) -> Result<(), Error> {
    //open rebuild file
    let f = File::open(src_rebuild)?;
    let schema = avro_rs::Schema::parse_str(SCHEMA)?;
    let reader = avro_rs::Reader::with_schema(&schema, &f)?;

    //open rebuild file (corrected)
    let fw = File::create(&dst_rebuild)?;
    let mut writer = Arc::new(Mutex::new(avro_rs::Writer::with_codec(
        &schema,
        fw,
        Codec::Snappy,
    )));

    let reader = reader.par_bridge();

    let failures = reader
        .map(|se| {
            let se = se?;
            let shards_rebuild: ShardEntry = avro_rs::from_value::<ShardEntryAvro>(&se)?.into();

            let shardentry_fixed = get_shard_line_starts(src_shards, shards_rebuild)?;

            // write it!
            let wtr_arc = writer.clone();
            let mut wtr_lock = wtr_arc.lock().unwrap();
            wtr_lock.append_ser::<ShardEntryAvro>(shardentry_fixed.into())?;

            Ok(())
        })
        .collect::<Result<Vec<()>, Error>>();

    if failures.is_err() {
        error!("{:?}", failures);
    }
    // // iterate on already generated avro file
    // for se in reader {
    //     // get entry
    // }
    Ok(())
}

fn get_shard_line_starts(
    src_shards: &Path,
    shards_rebuild: ShardEntry,
) -> Result<ShardEntry, Error> {
    // forge path and open related shard
    let mut shard_path = PathBuf::from(src_shards);
    shard_path.push(format!("{}.txt.gz", shards_rebuild.shard_id()));

    info!("working on shard {}", shards_rebuild.shard_id());

    let shard = Wet::from_path_gzip(shard_path)?;

    // iterate on the shard records
    let ret: Vec<BothLocation> = shard
        .iter
        .enumerate()
        .filter_map(|(idx, shard_record)| {
            //find records that are on both the shard and the rebuild
            match shards_rebuild
                .records()
                .iter()
                .find(|record_rebuild| record_rebuild.shard_record_number() == &idx)
            {
                Some(r) => {
                    // unwrap and filter like OSCAR v1.2
                    let shard_record = shard_record.unwrap();
                    let body_lines = shard_record
                        .body()
                        .lines()
                        .filter(|l| l.as_ref().unwrap().chars().count() > 100)
                        .map(|l| Some(l.as_ref().unwrap().trim_end().to_owned()));

                    // iteratively hash each sentence to find the one that starts the record
                    let line_start = body_lines
                        .enumerate()
                        .find(|(_, line)| {
                            let line = line.as_ref().unwrap();
                            let hash = hash_sentence(line);
                            r.start_hash() == &hash
                        })
                        // only get line index of matching line
                        .map(|(idx, _)| idx);

                    // clone location and update start_hash
                    // that will be used as record-level line offet (TODO: improve that)
                    let mut re = r.clone();
                    re.set_start_hash(line_start.unwrap() as u64);
                    Some(re)
                }
                None => None,
            }
        })
        .collect();

    // create a new shard entry
    let shardentry_fixed = ShardEntry::new(*shards_rebuild.shard_id(), ret);

    Ok(shardentry_fixed)
}
#[inline]
fn hash_sentence(s: &str) -> u64 {
    let mut hasher = XxHash64::default();
    hasher.write(s.as_bytes());
    hasher.finish()
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

    shard_number.unwrap()
}

fn shard_index(
    records: &HashMap<String, CorpusLocation>,
    src_shard: &Path,
) -> Result<ShardEntry, Error> {
    let shard_number = parse_shard_number(src_shard)?;
    process_shard(src_shard, shard_number, records)
}

fn process_shard(
    shard_path: &Path,
    shard_number: u64,
    records: &HashMap<String, CorpusLocation>,
) -> Result<ShardEntry, Error> {
    let shard = Wet::from_path_gzip(&shard_path)?;
    let mut ret = Vec::new();
    for (shard_record_number, shard_record) in shard.iter.enumerate() {
        let shard_record = shard_record?;
        let shard_record_id = shard_record.warc_id();
        if let Some(r) = records.get(shard_record_id) {
            ret.push(r.add_shard_loc(shard_record_id, shard_number, shard_record_number));
        }
    }

    Ok(ShardEntry::new(shard_number, ret))
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use warc::Record;

    use crate::processing::Metadata;

    use super::*;

    // #[test]
    // fn test_avro() {
    //     use std::{thread, time};
    //     // connect to pid
    //     println!("pid: {}", std::process::id());
    //     let ten_seconds = time::Duration::from_secs(10);
    //     thread::sleep(ten_seconds);
    //     let f = File::open("../data_test/100/rebuild/en.avro").unwrap();
    //     let schema =
    //         avro_rs::Schema::parse_list(&[SCHEMA_RECORD, SCHEMA_RECORD_LIST, SCHEMA_WHOLE])
    //             .unwrap();
    //     let reader = avro_rs::Reader::with_schema(&schema[2], &f).unwrap();
    //     let mut count = 0;
    //     for r in reader {
    //         count += 1;
    //         let r = r.unwrap();
    //         let r: HashMap<String, Vec<BothLocation>> = avro_rs::from_value(&r).unwrap();
    //         let mut count = 0;
    //         for (k, v) in r {
    //             println!("shard {} has {} records", k, v.len());
    //             count += v.len();
    //         }
    //         println!("{} records total", count);
    //     }
    //     println!("nb iter {}", count);
    // }
    // #[test]
    // fn test_avro_map_iter() {
    //     let f = File::open("../data_test/100/rebuild/en.avro").unwrap();
    //     let schema = avro_rs::Schema::parse_str(&SCHEMA).unwrap();
    //     let reader = avro_rs::Reader::with_schema(&schema, &f).unwrap();
    //     let mut count = 0;
    //     for r in reader {
    //         let r = r.unwrap();
    //         match r {
    //             avro_rs::types::Value::Map(hm) => {}
    //             _ => panic!("wrong type"),
    //         };
    //         count += 1;
    //     }
    //     println!("nb iter {}", count);
    // }
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
