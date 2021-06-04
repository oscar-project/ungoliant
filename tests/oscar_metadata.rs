use std::convert::TryFrom;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;

use flate2::Compression;
use ungoliant::error::Error;
use ungoliant::pipeline::Metadata;
use ungoliant::pipeline::OscarMetadata;
use ungoliant::shard;
use ungoliant::shard::wet;
use warc::header::WarcHeader;
use warc::RawRecord;

#[test]
//todo assert error type
fn pipeline_no_folders() {
    let src = PathBuf::from("svdkjljlkmjlmdsfljkf");
    let dst = PathBuf::from("fzjoijzoecijzoiej");

    let p = OscarMetadata::new(src, dst);
    assert!(p.run().is_err());
}

fn gen_test_shards(src: &Path, dst: &Path) -> Result<(), Box<dyn std::error::Error>> {
    for shard in std::fs::read_dir(src)? {
        let shard = shard?;
        let records = shard::wet::Wet::from_path_gzip(shard.path())?;
        let dst_path: PathBuf = [
            dst.to_str().unwrap(),
            &shard.file_name().into_string().unwrap(),
        ]
        .iter()
        .collect();
        let dst = File::create(dst_path)?;

        let mut buf = flate2::write::GzEncoder::new(dst, Compression::default());
        let mut writer = warc::WarcWriter::new(buf);
        for record in records.take(10) {
            let record = record?;
            writer.write_raw(&record)?;
        }
    }
    Ok(())
}

#[test]
#[ignore]
fn assert_meta_validity() {
    let mut src_gen = PathBuf::from("debug_1");
    let src = PathBuf::from("src_intg");
    let dst = PathBuf::from("dst_intg");
    gen_test_shards(&src_gen, &src).unwrap();
    let p = OscarMetadata::new(src.clone(), dst.clone());
    p.run().unwrap();

    let mut langfile_en = dst.clone();
    let mut metafile_en = dst.clone();
    langfile_en.push("en.txt");
    metafile_en.push("en_meta.json");

    println!("{:?}", langfile_en);

    // get data and metadata from generated µcorpus
    let mut langfile_en = File::open(langfile_en).unwrap();
    let metafile_en = File::open(metafile_en).unwrap();
    let mut sentences = String::new();
    langfile_en.read_to_string(&mut sentences).unwrap();
    let sentences: Vec<&str> = sentences.lines().collect();
    let metadata: Vec<Metadata> = serde_json::from_reader(metafile_en).unwrap();

    // get data and metadata from shard
    let mut source = src;
    source.push("0.txt.gz");
    let shard = wet::Wet::from_path_gzip(&source).unwrap();
    let shard_records: Vec<RawRecord> = shard.map(|x| x.unwrap()).collect();
    let shard_metadata: Vec<Metadata> = shard_records
        .iter()
        .map(|record| Metadata::try_from(record.headers.clone()).unwrap())
        .collect();

    for meta in metadata {
        let meta_match = shard_metadata
            .iter()
            .enumerate()
            // find matching record in shard
            .find(|(_, x)| {
                x.headers.get(&WarcHeader::RecordID).unwrap()
                    == meta.headers.get(&WarcHeader::RecordID).unwrap()
            })
            // ensure that is has only one language
            .and_then(|(idx, x)| {
                if !x
                    .headers
                    .get(&WarcHeader::Unknown(
                        "warc-identified-content-language".to_string(),
                    ))
                    .unwrap()
                    .contains(",")
                {
                    return Some((idx, x));
                }
                None
            });

        println!("{:?}", meta_match);
        if let Some(m) = meta_match {
            //TODO use hashsets instead of vecs so that
            //it is possible to compare sets
            // get lines from corpus
            // in a vec
            let corpus_lines: Vec<&&str> = sentences
                .iter()
                .skip(meta.offset)
                .take(meta.nb_sentences)
                .collect();

            // get lines from shard
            // in a vec
            let shard_string = String::from_utf8_lossy(&shard_records[m.0].body);
            let shard_lines: Vec<&str> = shard_string.lines().collect();

            //TODO continue
            //ensure that sentences from corpus are in shard
            println!("from corpus: {:#?}", corpus_lines);
            println!("from shard: {:#?}", shard_lines);
        }
    }
}
#[test]
#[ignore]
fn pipeline_single_shard() {
    let src = PathBuf::from("debug_1/");
    let dst = PathBuf::from("temp_1/");
    let tmp_test =
        std::fs::create_dir(&dst).expect("could not create temporary file for integration tests");

    let p = OscarMetadata::new(src.clone(), dst.clone());
    let res = p.run();
    assert!(res.is_ok());

    std::fs::remove_dir_all(dst);
}
#[test]
fn test_add() {
    assert_eq!(4, 2 + 2);
}
