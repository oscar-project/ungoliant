use std::{
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
            avro_schema::SCHEMA,
            location::Both,
            rebuilder::{ShardEntry, ShardEntryAvro},
        },
        Metadata,
    },
    sources::commoncrawl::Wet,
};
use log::debug;

/// builds a [PieceMeta] from a record and its [Both] location.
///
/// May fail if body contains invalid UTF-8 data or if the record has invalid headers.
fn build_piecemeta(record: Record<BufferedBody>, loc: &Both) -> Result<PieceMeta, Error> {
    let body = String::from_utf8(record.body().to_vec())?;
    let lines_kept = body
        .lines()
        .filter(|x| x.chars().count() > 100)
        .map(|x| x.trim_end())
        .skip(*loc.start_hash() as usize)
        .take(*loc.nb_sentences())
        .inspect(|x| println!("{}", x))
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

/// extracts a vector of [PieceMeta] from a given shard path (reading in it) following the provided [ShardEntry].
fn extract_from_shard(
    shard_entry: &ShardEntry,
    shard_path: &Path,
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
                let rec = match rec {
                    Ok(r) => r,
                    Err(e) => return Some(Err(e.into())),
                };

                //try to build piecemeta
                match build_piecemeta(rec, loc) {
                    Ok(pm) => Some(Ok(pm)),
                    Err(e) => Some(Err(e)),
                }
            }
            None => None,
        }
    });

    // collect into Vec
    // type hint is to use Results' into_iter trait
    records_from_shard.collect::<Result<Vec<PieceMeta>, Error>>()
}

/// rebuilding operation. Takes rebuild file(s) from `src_rebuild`, reads it and the rebuilds
/// corpora reading from `src_shards` into `dst`.
pub fn rebuild(src_rebuild: &Path, src_shards: &Path, dst: &Path) -> Result<(), Error> {
    // open avro rebuild file
    let f = File::open(src_rebuild)?;
    let schema = avro_rs::Schema::parse_str(SCHEMA)?;
    let reader = avro_rs::Reader::with_schema(&schema, &f)?;

    // open/create source corpus
    let mut langwriter = writer::Writer::new(dst, "en", None)?;
    let mut count = 0;

    for r in reader {
        //parse value
        let r = r?;
        let r: ShardEntry = avro_rs::from_value::<ShardEntryAvro>(&r)?.into();

        // extract pieces from shard and convert to merged pieces
        let pieces = extract_from_shard(&r, src_shards)?
            .into_iter()
            .map(|p| p.into())
            .collect();

        //write pieces
        langwriter.write(pieces)?;
        count += r.records().len();
        debug!("{} records total", count);
    }
    debug!("nb iter {}", count);
    Ok(())
}

#[cfg(test)]
mod tests {
    use warc::Record;

    use crate::processing::rebuild::{
        location::{Both, Corpus},
        rebuild::build_piecemeta,
    };

    #[test]
    fn test_build_piecemeta() {
        let mut r = Record::default();
        let sentences = r#"0 long but not in rebuild XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
1 valid and in rebuild XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
2 valid and in rebuild XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
3 too short! XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
4 valid and in rebuild XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
5 valid and in rebuild XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
6 valid but not in rebuild XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
7 valid but not in rebuild XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
8 valid but not in rebuild XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
9 valid but not in rebuild XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
        "#;
        let r = r.add_body(sentences);

        let mut corpusloc = Corpus::default();

        // nb of valid sentences
        corpusloc.set_nb_sentences(4);

        // start_hash is start offset when rebuilding.
        corpusloc.set_start_hash(1);

        let loc = corpusloc.add_shard_loc(r.warc_id(), 0, 0);
        let r = build_piecemeta(r, &loc).unwrap();

        let valid_line_numbers = ["1", "2", "4", "5"];
        for line in r.sentences {
            let line_number = line.split_once(' ').unwrap();
            assert!(valid_line_numbers.contains(&line_number.0));
        }
    }
}
