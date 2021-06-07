use std::collections::HashSet;
use std::convert::TryFrom;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;

use flate2::Compression;
use ungoliant::error::Error;
use ungoliant::lang::LANG;
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
        for record in records.take(200) {
            let record = record.unwrap();
            writer.write_raw(&record)?;
        }
    }
    Ok(())
}

#[test]
#[ignore]
fn assert_meta_validity() {
    // generate test shards
    // and run pipeline on them
    let mut src_gen = PathBuf::from("result_1");
    let src = PathBuf::from("src_intg");
    let dst = PathBuf::from("dst_intg");
    gen_test_shards(&src_gen, &src)
        .expect("ensure to have a folder named result_1 containing 0.txt.gz as test shard.");
    let p = OscarMetadata::new(src.clone(), dst.clone());
    p.run().unwrap();

    // get data and metadata from shard
    let mut source = src;
    source.push("0.txt.gz");
    let shard = wet::Wet::from_path_gzip(&source).unwrap();

    // unwrap and ignore errors
    let shard_records: Vec<RawRecord> = shard.filter_map(|x| x.ok()).collect();
    let shard_metadata: Vec<Metadata> = shard_records
        .iter()
        .map(|record| Metadata::try_from(record.headers.clone()).unwrap())
        .collect();

    for lang in LANG.iter() {
        // generate lang file paths
        let mut langfile = dst.clone();
        let mut metafile = dst.clone();
        langfile.push(format!("{}.txt", lang));
        metafile.push(format!("{}_meta.json", lang));

        // open sentence/metadata files
        let mut langfile = File::open(langfile).unwrap();
        let metafile = File::open(metafile).unwrap();

        // read sentences
        let mut sentences = String::new();
        langfile.read_to_string(&mut sentences).unwrap();

        // put sentences and metadata into vectors
        let sentences: Vec<&str> = sentences.lines().collect();
        let metadata: Vec<Metadata> = serde_json::from_reader(metafile).unwrap();

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
                        // silently fail condition
                        // not ideal
                        .unwrap_or(&",".to_string())
                        .contains(",")
                    {
                        return Some((idx, x));
                    }
                    None
                });

            // if there's a match
            if let Some(m) = meta_match {
                // take nb_sentences sentences from offset
                let corpus_lines: HashSet<&str> = sentences
                    .iter()
                    .skip(meta.offset)
                    .take(meta.nb_sentences)
                    // deref to &str
                    .map(|x| *x)
                    .collect();

                // get lines from shard
                // in a vec
                let shard_string = String::from_utf8_lossy(&shard_records[m.0].body);
                let shard_lines: HashSet<&str> = shard_string.lines().collect();

                // ensure that corpus is into shard
                assert!(corpus_lines.is_subset(&shard_lines));
            }
        }
    }
    std::fs::remove_dir_all(&dst).expect(&format!("could not delete test dst folder: {:?}", &dst));
}

#[test]
#[ignore]
fn pipeline_single_shard() {
    let src = PathBuf::from("debug_1/");
    let dst = PathBuf::from("temp_1/");

    let p = OscarMetadata::new(src.clone(), dst.clone());
    let res = p.run();
    assert!(res.is_ok());

    std::fs::remove_dir_all(dst);
}
