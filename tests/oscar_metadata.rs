use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

use ungoliant::error::Error;
use ungoliant::pipeline::Metadata;
use ungoliant::pipeline::OscarMetadata;
use ungoliant::shard;

#[test]
//todo assert error type
fn pipeline_no_folders() {
    let src = PathBuf::from("svdkjljlkmjlmdsfljkf");
    let dst = PathBuf::from("fzjoijzoecijzoiej");

    let p = OscarMetadata::new(src, dst);
    assert!(p.run().is_err());
}

#[test]
#[ignore]
fn assert_meta_validity() {
    let lang = File::open("dst/fr.txt").unwrap();
    let meta = File::open("dst/fr_meta.json").unwrap();

    let lang_header =
        warc::header::WarcHeader::Unknown("warc-identified-content-language".to_string());
    let record_id_header = warc::header::WarcHeader::RecordID;
    // idea is to iterate over metadata,
    // get id of record,
    // get offset + nb_sentences to fetch lines from langfile
    // find id in shard
    // check if lines are in record.
    // bonus: find all pieces of a said record and see if we can build it
    // let meta_reader = BufReader::from(meta);
    let metadata: Vec<Metadata> = serde_json::from_reader(meta).unwrap();
    let shard = shard::wet::Wet::from_path_gzip("debug_1/0.txt.gz").unwrap();
    for record in shard {
        let record = record.unwrap();
        let lang = record.headers.get(&lang_header);
        if lang == Some(&Vec::from("fra".as_bytes())) {
            println!("found a french record!")
            //TODO compare
        }
    }
    // for meta in metadata {
    //     let meta_id = meta
    //         .headers
    //         .get(&warc::header::WarcHeader::RecordID)
    //         .unwrap();

    //     for record in shard {
    //         let record = record.unwrap();
    //         let record_meta = Metadata::from(&record.headers);
    //         let record_id = String::from_utf8_lossy(
    //             record
    //                 .headers
    //                 .get(&warc::header::WarcHeader::RecordID)
    //                 .unwrap(),
    //         )
    //         .to_string();
    //         if &record_id == meta_id {
    //             println!("{:#?}",)
    //         }
    //     }
    // }
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
