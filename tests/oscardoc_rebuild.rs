// gen a tiny corpus from one shard
// get the rebuild files
// regen the corpus from the rebuild files
// ensure that the rebuilt corpus files are similar to the original ones (to the sha256sum)

use std::{
    fs::File,
    io::BufReader,
    path::{Path, PathBuf},
};
use test_log::test;

use ungoliant::{
    pipelines::{OscarDocNew as OscarDoc, Pipeline},
    processing::rebuild::Rebuilder,
};

use oscar_io::{self, v3::Document};
fn gen_corpus() {
    let src = Path::new("res/shards/").to_path_buf();
    let dst = Path::new("res/corpus/").to_path_buf();
    let lid = Path::new("lid.176.bin").to_path_buf();
    let bl = Path::new("res/blocklist/").to_path_buf();
    let kenlm = Path::new("res/kenlm/").to_path_buf();

    //TODO test with custom blocklists
    let pipeline = OscarDoc::new(src, dst, lid, Some(bl), Some(kenlm), None, false);
    pipeline.run().expect(
        "Ensure to have shards in res/shards, lid.176.bin at root and blocklist at res/blocklist",
    );
}

// TODO: Make it faster. It makes CI fail :(
// #[test]
#[cfg(not(tarpaulin))]
fn check_rebuild() {
    #[inline]
    fn get_record_id(doc: &Document) -> String {
        let rid_bytes = doc.warc_headers().get(&warc::WarcHeader::RecordID).unwrap();
        String::from_utf8_lossy(rid_bytes).to_string()
    }

    gen_corpus();

    // rebuild french corpus
    let src_rebuild = Path::new("res/corpus/rebuild/fr.avro");
    let src_corpus = Path::new("res/corpus/fr_meta.jsonl");
    let src_shards = Path::new("res/shards");
    let mut dst = PathBuf::from("res/rebuilt");
    let lang = oxilangtag::LanguageTag::parse("fr".to_string()).unwrap();
    let rb = Rebuilder::new(src_rebuild, src_shards, &dst, lang);
    rb.run().unwrap();

    // open source corpus, store documents and order them by record id
    let doc_reader_source = oscar_io::v3::Reader::from_path(&src_corpus).unwrap();
    let mut docs_source = doc_reader_source.map(|x| x.unwrap()).collect::<Vec<_>>();
    docs_source.sort_unstable_by(|a, b| get_record_id(a).cmp(&get_record_id(b)));
    // open rebuilt corpus
    dst.push("fr_meta.jsonl");
    let doc_reader_rebuild = oscar_io::v3::Reader::from_path(&dst).unwrap();
    let mut docs_rebuild = doc_reader_rebuild.map(|x| x.unwrap()).collect::<Vec<_>>();
    docs_rebuild.sort_unstable_by(|a, b| get_record_id(a).cmp(&get_record_id(b)));

    for (ds, dr) in docs_source.iter().zip(&docs_rebuild) {
        assert_eq!(ds, dr);
    }
}
