use criterion::{black_box, criterion_group, criterion_main, Criterion};
use itertools::Itertools;
use rayon::prelude::*;
use ungoliant::classify::Classifier;
use ungoliant::wet::Wet;
use warc::{header::WarcHeader, RawRecord};

const NB_RECORDS: usize = 100;
// bench protocol:
//
// We take 4 files (TODO: 8 in order to have files>cores/threads)
// We take 100 records per file
//
// - Full sequential
// - Sequential on WET and Records, Parallel on Sentences
// - Sequential on WET and Sentences, Parallel on Records
// - Parallel on WET, Sequential on Sentences and Records

// Full sequential
pub fn pipeline_wet_seq_rec_seq_sen_seq(c: &mut Criterion) {
    fn classify_all() {
        let lang_tag = WarcHeader::Unknown("warc-identified-content-language".to_string());
        let cls = Classifier::new_lid().unwrap();
        let results = std::fs::read_dir("results/")
            .unwrap()
            .map(|d| Wet::from_path_gzip(d.unwrap().path()).unwrap());

        for wetfile in results {
            for record in wetfile.take(NB_RECORDS) {
                let record = record.unwrap();
                let body = String::from_utf8(record.body).ok();
                if let Some(sentences) = body {
                    let sentences = sentences.lines().filter(|line| line.chars().count() > 100);
                    for sentence in sentences {
                        cls.predict(sentence);
                    }
                }
            }
        }
    }
    c.bench_function("pipeline wet=seq rec=seq line=seq ", |b| {
        b.iter(|| black_box(classify_all()))
    });
}

// Sequential on WET files and records, concurrent on lines.
pub fn pipeline_wet_seq_rec_seq_sen_par(c: &mut Criterion) {
    fn classify_all() {
        let lang_tag = WarcHeader::Unknown("warc-identified-content-language".to_string());
        let cls = Classifier::new_lid().unwrap();
        let results = std::fs::read_dir("results/")
            .unwrap()
            .map(|d| Wet::from_path_gzip(d.unwrap().path()).unwrap());
        for wetfile in results {
            for record in wetfile.take(NB_RECORDS) {
                let record = record.unwrap();
                let body = String::from_utf8(record.body).ok();
                if let Some(sentences) = body {
                    let sentences: Vec<&str> = sentences
                        .lines()
                        .filter(|line| line.chars().count() > 100)
                        .collect();
                    sentences.par_iter().for_each(|sentence| {
                        cls.predict(sentence);
                    });
                }
            }
        }
    }
    c.bench_function("pipeline wet=seq rec=seq line=par ", |b| {
        b.iter(|| black_box(classify_all()))
    });
}

// parallel on records
pub fn pipeline_wet_seq_rec_par_sen_seq(c: &mut Criterion) {
    fn classify_all() {
        let lang_tag = WarcHeader::Unknown("warc-identified-content-language".to_string());
        let cls = Classifier::new_lid().unwrap();
        let results = std::fs::read_dir("results/")
            .unwrap()
            .map(|d| Wet::from_path_gzip(d.unwrap().path()).unwrap());
        for wetfile in results {
            let records = wetfile.into_iter().take(NB_RECORDS).par_bridge();
            records.for_each(|record| {
                let record = record.unwrap();
                let body = String::from_utf8(record.body).ok();
                if let Some(sentences) = body {
                    let sentences: Vec<&str> = sentences
                        .lines()
                        .filter(|line| line.chars().count() > 100)
                        .collect();
                    sentences.iter().for_each(|sentence| {
                        cls.predict(sentence);
                    });
                }
            });
        }
    }
    c.bench_function("pipeline wet=seq rec=par line=seq ", |b| {
        b.iter(|| black_box(classify_all()))
    });
}

// parallel on WET
pub fn pipeline_wet_par_rec_seq_sen_seq(c: &mut Criterion) {
    fn classify_all() {
        let lang_tag = WarcHeader::Unknown("warc-identified-content-language".to_string());
        let cls = Classifier::new_lid().unwrap();
        let results = std::fs::read_dir("results/")
            .unwrap()
            .map(|d| Wet::from_path_gzip(d.unwrap().path()).unwrap());
        let results = results.par_bridge();
        results.for_each(|wetfile| {
            let records = wetfile.take(NB_RECORDS);
            for record in records {
                let record = record.unwrap();
                let body = String::from_utf8(record.body).ok();
                if let Some(sentences) = body {
                    for sentence in sentences.lines().filter(|line| line.chars().count() > 100) {
                        cls.predict(sentence);
                    }
                }
            }
        });
    }
    c.bench_function("pipeline wet=par rec=seq line=seq ", |b| {
        b.iter(|| black_box(classify_all()))
    });
}

criterion_group!(
    benches,
    pipeline_wet_seq_rec_seq_sen_seq,
    pipeline_wet_seq_rec_seq_sen_par,
    pipeline_wet_seq_rec_par_sen_seq,
    pipeline_wet_par_rec_seq_sen_seq
);
criterion_main!(benches);
