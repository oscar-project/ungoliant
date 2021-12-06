use std::collections::HashMap;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ungoliant::{
    identifiers::FastText,
    pipelines::oscardoc::types::{Document, Metadata},
    transformers::{Annotate, Noisy},
};
pub fn noisy(c: &mut Criterion) {
    let mut documents: Vec<Document> = [
        "//////////////////////////////////////////////.",
        "lorem ipsum dolor sit ////////////////////////.",
        "lore////mmm////m ipsum d///////olor//////sit a.",
        "lorem ipsum dolor sit amet.",
    ]
    .into_iter()
    .map(String::from)
    .map(|content| Document::new(content, HashMap::new(), Metadata::default()))
    .collect();
    let a = Noisy::default();
    c.bench_function("noisy_annotate", |b| {
        b.iter(|| {
            let documents = documents.clone();
            for mut d in documents {
                a.annotate(black_box(&mut d))
            }
        })
    });
}

criterion_group!(benches, noisy);
criterion_main!(benches);
