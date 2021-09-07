use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ungoliant::identifiers::FastText;
pub fn fasttext_benchmark(c: &mut Criterion) {
    let cls = FastText::new_lid().unwrap();
    let dummy = "This is a completely innocent phrase 
    This is a completely innocent phrase 
    This is a completely innocent phrase 
    This is a completely innocent phrase 
    This is a completely innocent phrase 
    This is a completely innocent phrase 
    This is a completely innocent phrase 
    This is a completely innocent phrase 
    This is a completely innocent phrase 
    This is a completely innocent phrase 
    This is a completely innocent phrase 
    This is a completely innocent phrase 
    This is a completely innocent phrase 
    This is a completely innocent phrase 
    This is a completely innocent phrase 
    This is a completely innocent phrase 
    This is a completely innocent phrase ";
    c.bench_function("fasttext", |b| b.iter(|| cls.predict(black_box(dummy))));
}

criterion_group!(benches, fasttext_benchmark);
criterion_main!(benches);
