use std::path::Path;

use ctclib::{Dict, KenLM, Model, LM};

const MODEL_PATH: &str = "kenlm_mini.arpa";
fn main() {
    let dict = Dict::new();
    let mut model = Model::new(&Path::new(MODEL_PATH)).unwrap();
    let vocab = model.vocab();
    let kind_sentence = "This is a perfectly valid sentence";
    let total_score = perplexity(&mut model, kind_sentence);
    {
        let mut model = KenLM::new(&Path::new(MODEL_PATH), &Dict::new()).unwrap();
        let total_score = model.perplexity(kind_sentence);
        println!("kind: {total_score}");
    }
    println!("kind: {total_score}");
    let not_kind_sentence = r#"<put nsfw sentence here>"#;
    let total_score = perplexity(&mut model, not_kind_sentence);
    println!("not kind: {total_score}");
}

/// Compute perplexity of a sentence.
/// # params:
/// - sentence One full sentence to score.  Do not include <s> or </s>.
#[inline]
fn perplexity(model: &mut Model, sentence: &str) -> f32 {
    let nb_words = sentence.split_whitespace().count() as f32 + 1f32; // account for </s>

    10f32.powf(-score(model, sentence) / nb_words)
}

fn score(model: &mut Model, sentence: &str) -> f32 {
    let tokens: Vec<&str> = sentence.split_whitespace().collect();
    let token_ids: Vec<_> = tokens.iter().map(|tok| model.vocab().index(tok)).collect();
    let mut total = 0f32;

    let mut state = model.begin_context();
    for (token, token_id) in tokens.iter().zip(token_ids) {
        let (new_state, score) = model.base_score(&state, token_id);
        total += score;
        state = new_state;
        //println!("\t{token}({token_id}) -> {score}");
    }
    let (_, score) = model.base_score(&state, model.vocab().end_sentence());
    total + score
}
