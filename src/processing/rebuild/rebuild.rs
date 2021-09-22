use std::{
    collections::HashMap,
    convert::TryFrom,
    fs::File,
    path::{Path, PathBuf},
};

use warc::{BufferedBody, Record};

use crate::{
    error::Error,
    io::{reader::reader::PieceMeta, writer},
    processing::{
        rebuild::{
            avro_schema::{SCHEMA, SCHEMA_RECORD, SCHEMA_RECORD_LIST, SCHEMA_WHOLE},
            location::Both,
            rebuilder::{ShardEntry, ShardEntryAvro},
        },
        Metadata,
    },
    sources::commoncrawl::Wet,
};
use std::{thread, time};

fn build_piecemeta(record: Record<BufferedBody>, loc: &Both) -> Result<PieceMeta, Error> {
    let body = String::from_utf8(record.body().to_vec())?;
    let lines_kept = body
        .lines()
        .filter(|x| x.chars().count() > 100)
        .map(|x| x.trim_end())
        .skip(*loc.start_hash() as usize)
        .take(*loc.nb_sentences())
        .map(String::from);

    let mut metadata: Metadata = Metadata::try_from(record.into_raw_parts().0.headers)?;
    metadata.nb_sentences = *loc.nb_sentences();

    let pm = PieceMeta {
        sentences: lines_kept.collect(),
        headers: metadata,
        identification: "en",
    };

    Ok(pm)
}

fn extract_from_shard(
    shard_entry: &ShardEntry,
    shard_path: &Path,
    sort: bool,
) -> Result<Vec<PieceMeta>, Error> {
    //forge shard_path
    let mut shard_path = PathBuf::from(shard_path);
    shard_path.push(format!("{}.txt.gz", shard_entry.shard_id()));

    let shard_reader = Wet::from_path_gzip(shard_path)?;

    let records_from_shard = shard_reader.iter.enumerate().filter_map(|(idx, rec)| {
        // try to find current record (from shard) in rebuild file (shard_entry).
        match shard_entry
            .records()
            .iter()
            .find(|x| x.shard_record_number() == &idx)
        {
            // if we find the related shard entry, extract sentences/metadata and build a Piecemeta
            // for writing
            Some(loc) => {
                let rec = rec.unwrap();
                let pm = build_piecemeta(rec, loc).unwrap();
                Some(pm)
            }
            None => None,
        }
        // if record_numbers.contains(&&idx) {
        //     println!("{:?}", )
        //     Some(rec)
        // } else {
        //     None
        // }
    });

    // for se in records_from_shard {
    //     let se = se;
    //     println!("{:?}", se.warc_id());
    // }

    Ok(records_from_shard.collect())
}

pub fn rebuild(src_rebuild: &Path, src_shards: &Path, dst: &Path) -> Result<(), Error> {
    // open avro rebuild file
    let f = File::open(src_rebuild).unwrap();
    let schema = avro_rs::Schema::parse_str(&SCHEMA).unwrap();
    let reader = avro_rs::Reader::with_schema(&schema, &f).unwrap();

    let mut langwriter = writer::Writer::new(dst, "en", None)?;
    let mut count = 0;
    for r in reader {
        // unwrap/parse value
        let r = r.unwrap();
        let r: ShardEntry = avro_rs::from_value::<ShardEntryAvro>(&r).unwrap().into();

        // extract pieces from shard and convert to merged pieces
        let pieces = extract_from_shard(&r, src_shards, false)?
            .into_iter()
            .map(|p| p.into())
            .collect();

        //write pieces
        langwriter.write(pieces);
        count += r.records().len();
        println!("{} records total", count);
    }
    println!("nb iter {}", count);
    Ok(())
}
