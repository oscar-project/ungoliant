use criterion::{black_box, criterion_group, criterion_main, Criterion};
use itertools::Itertools;
use rayon::prelude::*;
use ungoliant::identifiers::FastText;
use ungoliant::sources::commoncrawl::Wet;
use warc::{BufferedBody, Record, WarcHeader};

// bench protocol:
//
// We take 4 files (TODO: 8 in order to have files>cores/threads)
// We take 1000 records per file
//
// Strategies to test:
// - Full sequential
// - Sequential on wet files, concurrent/parallel on lines:
//    - par_iter() on lines, without chunking
//    - with chunking
// - Parallel on wet files, sequential on lines
// - Parallel on both:
//    - par_iter() on lines, without chunking
//    - with chunking

// Full sequential
pub fn pipeline_full_sequential_benchmark(c: &mut Criterion) {
    fn parse_headers() {
        let lang_tag = WarcHeader::Unknown("warc-identified-content-language".to_string());
        let cls = FastText::new_lid().unwrap();
        let results = std::fs::read_dir("results/")
            .unwrap()
            .map(|d| Wet::from_path_gzip(d.unwrap().path()).unwrap());

        for wetfile in results {
            for record in wetfile.iter.take(1000) {
                let record = record.unwrap();
                let body = String::from_utf8(record.body().to_vec()).ok();
                if let Some(sentences) = body {
                    let sentences = sentences.lines().filter(|line| line.chars().count() > 100);
                    for sentence in sentences {
                        cls.predict(sentence);
                    }
                }
            }
        }
    }
    c.bench_function("pipeline single ", |b| {
        b.iter(|| black_box(parse_headers()))
    });
}

pub fn pipeline_multithread_benchmark(c: &mut Criterion) {
    fn parse_headers() {
        let records = Wet::from_path_gzip("results/0.txt.gz").unwrap();

        let lang_tag = WarcHeader::Unknown("warc-identified-content-language".to_string());
        for c in records.iter.take(100).chunks(4).into_iter() {
            let c: Vec<Record<BufferedBody>> =
                c.filter(Result::is_ok).map(Result::unwrap).collect();

            let c = c
                .par_iter()
                .for_each(|record| match record.header(lang_tag.clone()) {
                    Some(lang) => {
                        String::from_utf8_lossy(lang.as_bytes());
                    }
                    None => (),
                });
        }
    }
    c.bench_function("pipeline multi chunk.len()==4", |b| {
        b.iter(|| black_box(parse_headers()))
    });
}

pub fn pipeline_multithread2_benchmark(c: &mut Criterion) {
    fn parse_headers() {
        // let records = Wet::from_path_gzip("results/0.txt.gz").unwrap();

        // let lang_tag = WarcHeader::Unknown("warc-identified-content-language".to_string());
        // let chunks = records.take(100).chunks(25);
        // chunks.par_iter();
        // for c in records.take(100).chunks(4).into_iter() {
        //     let c : Vec<RawRecord> = c
        //         .filter(Result::is_ok)
        //         .map(Result::unwrap).collect();

        //     let c = c.par_iter().for_each(|record| {
        //         match record.headers.get(&lang_tag) {
        //             Some(lang) => {String::from_utf8_lossy(lang);}
        //             None => (),
        //         }
        //     });
        // }
    }
    c.bench_function("pipeline multi chunk.len()==4", |b| {
        b.iter(|| black_box(parse_headers()))
    });
}

criterion_group!(
    benches,
    pipeline_full_sequential_benchmark,
    pipeline_multithread_benchmark
);
criterion_main!(benches);
