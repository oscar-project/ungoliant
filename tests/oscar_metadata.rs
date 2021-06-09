use std::collections::HashMap;
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
        for record in records.skip(1).take(100) {
            let record = record.unwrap();
            writer.write_raw(&record)?;
        }
    }
    Ok(())
}

fn get_lang_data(lang: &'static str, dst: &PathBuf) -> (Vec<String>, Vec<Metadata>) {
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
    let sentences: Vec<String> = sentences.lines().map(|x| x.to_string()).collect();
    let metadata: Vec<Metadata> = serde_json::from_reader(metafile).unwrap();

    (sentences, metadata)
}

#[test]
#[ignore]
fn assert_meta_final_offset() {
    // generate test shards
    // and run pipeline on them
    let mut src_gen = PathBuf::from("result_1");
    let src = PathBuf::from("src_intg");
    let dst = PathBuf::from("dst_intg");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir(&dst).unwrap();
    gen_test_shards(&src_gen, &src)
        .expect("ensure to have a folder named result_1 containing 0.txt.gz as test shard.");
    let p = OscarMetadata::new(src.clone(), dst.clone());
    p.run().unwrap();

    for lang in LANG.iter() {
        // generate lang file paths
        let mut langfile = dst.clone();
        let mut metafile = dst.clone();
        langfile.push(format!("{}.txt", lang));
        metafile.push(format!("{}_meta.json", lang));

        // open sentence/metadata files
        let mut langfile = File::open(langfile).unwrap();
        let metafile = File::open(metafile).unwrap();

        // get number of sentences from corpus
        let mut sentences = String::new();
        langfile.read_to_string(&mut sentences).unwrap();
        let nb_sentences_corpus = sentences.lines().count();

        let metadata: Vec<Metadata> = serde_json::from_reader(metafile).unwrap();

        // get final offset + nb_sentences
        let nb_sentences_metadata = match metadata.last() {
            Some(meta) => meta.offset + meta.nb_sentences,
            None => 0,
        };

        assert_eq!(nb_sentences_corpus, nb_sentences_metadata);
    }

    std::fs::remove_dir_all(&src).unwrap();
    std::fs::remove_dir_all(&dst).unwrap();
}

#[test]
#[ignore]
fn assert_meta_successive_offsets() {
    // generate test shards
    // and run pipeline on them
    let mut src_gen = PathBuf::from("result_1");
    let src = PathBuf::from("src_intg");
    let dst = PathBuf::from("dst_intg");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir(&dst).unwrap();
    gen_test_shards(&src_gen, &src)
        .expect("ensure to have a folder named result_1 containing 0.txt.gz as test shard.");
    let p = OscarMetadata::new(src.clone(), dst.clone());
    p.run().unwrap();

    for lang in LANG.iter() {
        // generate lang file paths
        let mut langfile = dst.clone();
        let mut metafile = dst.clone();
        langfile.push(format!("{}.txt", lang));
        metafile.push(format!("{}_meta.json", lang));

        // open sentence/metadata files
        let mut langfile = File::open(langfile).unwrap();
        let metafile = File::open(metafile).unwrap();

        // get number of sentences from corpus
        let mut sentences = String::new();
        langfile.read_to_string(&mut sentences).unwrap();
        let nb_sentences_corpus = sentences.lines().count();

        let metadata: Vec<Metadata> = serde_json::from_reader(metafile).unwrap();
        let nb_sentences_metadata = metadata.iter().fold(0, |acc, x| {
            assert_eq!(acc, x.offset);
            acc + x.nb_sentences
        });

        println!(
            "{}: C:{} M:{}",
            lang, nb_sentences_corpus, nb_sentences_metadata
        );
        assert_eq!(nb_sentences_corpus, nb_sentences_metadata);
    }
    std::fs::remove_dir_all(&src);
    std::fs::remove_dir_all(&dst);
}

#[test]
#[ignore]
fn assert_meta_validity() {
    // generate test shards
    // and run pipeline on them
    let mut src_gen = PathBuf::from("result_1");
    let src = PathBuf::from("src_intg");
    let dst = PathBuf::from("dst_intg");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir(&dst).unwrap();
    gen_test_shards(&src_gen, &src)
        .expect("ensure to have a folder named result_1 containing 0.txt.gz as test shard.");
    let p = OscarMetadata::new(src.clone(), dst.clone());
    p.run().unwrap();

    // get data and metadata from shard
    let mut source = src.clone();
    source.push("0.txt.gz");
    let shard = wet::Wet::from_path_gzip(&source).unwrap();

    let f = File::open(&source).unwrap();
    let reader = flate2::read::GzDecoder::new(&f);
    let breader = BufReader::new(reader);
    let shard = wet::Wet::new(breader);

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
    std::fs::remove_dir_all(&src).expect(&format!("could not delete test src folder: {:?}", &dst));
    std::fs::remove_dir_all(&dst).expect(&format!("could not delete test dst folder: {:?}", &dst));
}

#[test]
#[ignore]
fn assert_meta_final_offset_multishard() {
    // generate test shards
    // and run pipeline on them
    let mut src_gen = PathBuf::from("result_2");
    let src = PathBuf::from("src_intg");
    let dst = PathBuf::from("dst_intg");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir(&dst).unwrap();
    gen_test_shards(&src_gen, &src)
        .expect("ensure to have a folder named result_1 containing 0.txt.gz as test shard.");
    let p = OscarMetadata::new(src.clone(), dst.clone());
    p.run().unwrap();

    for lang in LANG.iter() {
        // generate lang file paths
        let mut langfile = dst.clone();
        let mut metafile = dst.clone();
        langfile.push(format!("{}.txt", lang));
        metafile.push(format!("{}_meta.json", lang));

        // open sentence/metadata files
        let mut langfile = File::open(langfile).unwrap();
        let metafile = File::open(metafile).unwrap();

        // get number of sentences from corpus
        let mut sentences = String::new();
        langfile.read_to_string(&mut sentences).unwrap();
        let lines = sentences.lines();
        let nb_sentences_corpus = lines.count();

        let mut metadata: Vec<Metadata> = serde_json::from_reader(metafile).unwrap();

        //sort metadata
        //to get the one that has the last offset
        metadata.sort_by(|a, b| a.offset.cmp(&b.offset));

        for m in &metadata {
            println!("{:#?}", m);
            println!("{}+{}", m.offset, m.nb_sentences);
        }
        // get final offset + nb_sentences
        let nb_sentences_metadata = match metadata.last() {
            Some(meta) => meta.offset + meta.nb_sentences,
            None => 0,
        };

        assert_eq!(nb_sentences_corpus, nb_sentences_metadata);
    }

    std::fs::remove_dir_all(&src).unwrap();
    std::fs::remove_dir_all(&dst).unwrap();
}
#[test]
#[ignore]
fn assert_meta_validity_multishard() {
    // gen test shards and run pipeline
    let mut src_gen = PathBuf::from("result_5");
    let src = PathBuf::from("src_intg");
    let dst = PathBuf::from("dst_intg");
    std::fs::create_dir(&src);
    std::fs::create_dir(&dst);
    let src_prod = PathBuf::from("result_2");
    gen_test_shards(&src_gen, &src)
        .expect("ensure to have a folder named result_5 containing 0.txt.gz as test shard.");
    let p = OscarMetadata::new(src.clone(), dst.clone());
    p.run().unwrap();

    let mut shard_records = HashMap::new();
    //Read all 5 shards
    for shard_idx in 0..5 {
        let mut shard_path = src.clone();
        shard_path.push(format!("{}.txt.gz", shard_idx));
        println!("processing {:?}", &shard_path);
        // let shard = wet::Wet::from_path_gzip(&shard_path).unwrap();
        let f = File::open(&shard_path).unwrap();
        let bf = BufReader::new(f);
        let reader = flate2::bufread::MultiGzDecoder::new(bf);
        let breader = BufReader::new(reader);
        let shard = wet::Wet::new(breader);

        // store shards in hashmap
        for record in shard {
            if let Ok(record) = record {
                if let Some(r_id) = record.headers.get(&WarcHeader::RecordID) {
                    let record_id = String::from_utf8(r_id.clone()).expect("invalid record id");
                    shard_records.insert(record_id, record);
                } else {
                    println!("no record id : {:#?}", Metadata::try_from(record.headers));
                }
            } else {
                println!("error with record: {:?}", record.err());
            }
        }
    }

    println!("got {} records", &shard_records.len());

    let mut nb_records_from_corpus = 0;

    for lang in LANG.iter() {
        // get from corpus
        let (sentences, mut metadata) = get_lang_data(lang, &dst);
        metadata.sort_by(|a, b| a.offset.cmp(&b.offset));
        // check number of sentence groups

        let nb_sentences_from_metadata = {
            if sentences.len() == 0 && metadata.len() == 0 {
                0
            } else {
                if let Some(m) = metadata.last() {
                    m.offset + m.nb_sentences
                } else {
                    0
                }
            }
        };

        if sentences.len() != nb_sentences_from_metadata {
            println!("sentences and metadata number not equal for lang {}", lang);
        }

        nb_records_from_corpus += metadata.len();

        for (corpus_metadata, corpus_sentences) in metadata.iter().zip(sentences) {
            // get record id of current metadata
            let corpus_record_id = corpus_metadata.headers.get(&WarcHeader::RecordID);
            if corpus_record_id.is_none() {
                println!("no record id : {:#?}", &corpus_metadata.headers);
            };
            let corpus_record_id = corpus_record_id.unwrap();

            // get matching record from shard-extracted data

            let shard_record = shard_records.get(corpus_record_id);
            if shard_record.is_none() {
                println!("record id not in shards: {:#?}", &corpus_record_id);
            }
            let shard_record = shard_record.unwrap();

            //ensure that corpus_record is subset of shard_record
            let corpus_sentences: HashSet<&str> = corpus_sentences.lines().collect();
            let corpus_sentences: HashSet<&str> = corpus_sentences
                .into_iter()
                .skip(corpus_metadata.offset)
                .take(corpus_metadata.nb_sentences)
                .collect();
            let shard_sentences = String::from_utf8(shard_record.body.clone()).unwrap();
            let shard_sentences: HashSet<&str> = shard_sentences.lines().collect();

            println!(
                "record_id: {:#?}",
                corpus_metadata.headers.get(&WarcHeader::RecordID)
            );
            assert!(corpus_sentences.is_subset(&shard_sentences));
        }
    }
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

//TODO: test multilingual
