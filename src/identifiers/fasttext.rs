//! Fasttext identifier
use std::{collections::HashMap, path::Path, str::Lines};

use crate::{error::Error, lang::Lang};
use fasttext::{FastText as FastTextLib, Prediction};

use super::{identifier, Identification, Identifier};

/// Clean the prediction label field from `__label__xx` into `xx`.
///
/// Be aware that the function only skips 9 chars without doing any parsing,
/// so it may silently fail if `prediction.label.chars().count() > 9`
/// but not of a `__label__xx` form.
///
/// # Errors
/// Returns an error if provided prediction is too short to be cleaned.
fn clean_prediction(prediction: &Prediction) -> Result<Prediction, String> {
    if prediction.label.chars().count() < 9 {
        return Err(format!(
            "Label is too short to be cleaned: {}",
            prediction.label
        ));
    }
    Ok(Prediction {
        prob: prediction.prob,
        label: prediction.label.chars().skip(9).collect(),
    })
}

/// Holds a [fasttext::FastText] instance and its parameters:
/// - [fasttext::FastText::k], number of predicted languages on a sentence
/// - [FastText::threshold], prediction threshold
pub struct FastText {
    predictor: FastTextLib,
    pub k: i32,
    pub threshold: f32,
}

impl FastText {
    /// Create a new fasttext classifier allowing to identify
    /// language of strings.
    ///
    /// - [Self::k] is set to 1
    /// - [Self::threshold] is set to .8
    ///
    /// **Having `lid.176.bin` at `.` is mandatory**
    ///
    /// # Errors
    /// Propagates [fasttext::FastText] errors.
    pub fn new_lid() -> Result<Self, Error> {
        Self::new(Path::new("lid.176.bin"), 1, 0.8)
    }

    /// Create a new fasttext classifier.
    ///
    /// filename has to be a path to a `bin` file.
    ///
    /// See [fasttext::FastText::predict] for other parameters explanation
    pub fn new(filename: &Path, k: i32, threshold: f32) -> Result<Self, Error> {
        let mut predictor = FastTextLib::new();
        let filename_str = filename.to_str();
        match filename_str {
            None => Err(Error::Custom(format!(
                "invalid filepath for lid: {:?}",
                filename
            ))),
            Some(filename) => {
                predictor.load_model(filename)?;
                Ok(Self {
                    predictor,
                    k,
                    threshold,
                })
            }
        }
    }

    /// predict for supplied sentence.
    /// returns Ok(None) if no reliable identification has been done.
    pub fn predict(&self, sentence: &str) -> Result<Option<Vec<Prediction>>, String> {
        let predictions = self.predictor.predict(sentence, self.k, self.threshold)?;

        if predictions.is_empty() {
            Ok(None)
        } else {
            // attempt to clean labels before returning
            Ok(Some(
                predictions
                    .into_iter()
                    .map(|p| clean_prediction(&p).unwrap_or(p))
                    .collect(),
            ))
        }
    }

    /// Identifies each line, then returns both identifications for each line _and_
    /// a HashMap holding (byte_count, sum(byte_count*prob) / total count).
    pub fn get_weighted_ids(
        &self,
        lines: Lines,
    ) -> Result<
        (
            Vec<Option<Identification>>,
            HashMap<Option<Lang>, (usize, f32)>,
            usize,
        ),
        Error,
    > {
        // per-lang and total byte counts
        // lang_count maps Lang -> (lang_byte_count, sum(byte_count*prob))
        let mut lang_count = HashMap::new();
        let mut total_count = 0;

        // filter out unicode null chars
        // this prevents fasttext errors and hopefully improves
        // corpus quality
        let lines = lines.map(|l| l.replace(char::from(0), ""));
        let ids: Vec<Option<Identification>> = lines
            .map(|line| {
                // identify
                let id = self.identify(line.as_str());

                // add to byte count for document-level identification
                if let Ok(ref ide) = id {
                    // map Identification to its lang, or keep None to store the "None" language identification
                    let ide_label = ide.as_ref().map(|i| *i.label());
                    let ide_prob = ide.as_ref().map(|i| *i.prob());
                    // get length of current line
                    let byte_count = line.bytes().count();

                    lang_count
                        .entry(ide_label)
                        .and_modify(|(count, count_times_prob)| {
                            *count += byte_count;
                            *count_times_prob += byte_count as f32 * ide_prob.unwrap_or(1.0f32);
                        })
                        .or_insert((byte_count, byte_count as f32 * ide_prob.unwrap_or(1.0f32)));

                    total_count += byte_count;
                }
                id
            })
            .collect::<Result<_, Error>>()?;

        // divide by total count to get probs between 0 and 1.
        for (_, count_times_prob) in lang_count.values_mut() {
            *count_times_prob /= total_count as f32;
        }

        Ok((ids, lang_count, total_count))
    }
}

impl identifier::Identifier<&str> for FastText {
    fn identify(&self, sentence: &str) -> Result<Option<Identification>, Error> {
        let prediction = self
            .predictor
            .predict(sentence, 1, self.threshold)
            .map_err(Error::FastText)?;
        // let prediction = prediction.sort_by(|a, b| a.prob.partial_cmp(&b.prob)).iter().take(1);

        if !prediction.is_empty() {
            // TODO: There should be a solution without resorting to clone()
            let prediction = prediction[0].clone();
            Ok(Some(prediction.into()))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ambiguous/multilingual sentence that shouldn't yield a single lang with a high confidence
    #[test]
    fn test_no_id() {
        let classifier = FastText::new_lid().expect("could not instantiate a classifier");
        let short_sentence = "Bonjour Hello";
        let id = classifier
            .predict(short_sentence)
            .expect("could not predict sentence");
        println!("{:?}", id);
        assert!(id.is_none());
    }

    // unilingual longish sentence that should yield a single lang with a high confidence
    #[test]
    fn test_id_en() {
        let classifier = FastText::new_lid().expect("could not instantiate a classifier");
        let sentence = "a perfectly, innocent, quite lengthy sentence. How lengthy and normal this sentence is, oh my! Lengthy lengthy.".escape_default().to_string();
        let pred = classifier
            .predict(&sentence)
            .expect("could not launch prediction")
            .unwrap();
        assert_eq!(pred.len(), 1);
        let pred = &pred[0];
        assert_eq!(pred.label, "en");
    }
    // test that garbage unicode from CC does not procees to crash the underlying C++ code.
    // when escaped with C++ friendly escape_default() method.
    #[test]
    fn test_garbage() {
        use std::fs;
        let garbage_default = fs::read_to_string("tests/res/garbage.txt")
            .expect("could not find test file")
            .escape_default()
            .to_string();
        let classifier = FastText::new_lid().expect("could not instantiate a classifier");
        classifier
            .predict(&garbage_default)
            .expect("could not predict sentence");
    }

    // ensures that any null character in string
    // does not crash classifier.
    #[test]
    fn test_null_terminated() {
        let classifier = FastText::new_lid().expect("could not instantiate a classifier");
        let nullstring = String::from(char::from(0));
        let mut nullstring2 = String::from("hello");
        nullstring2.push(char::from(0));
        nullstring2.push_str(" world!");

        let cls1 = classifier.predict(&nullstring);

        let cls2 = classifier.predict(&nullstring);

        assert!(cls1.is_err());
        assert!(cls2.is_err());
    }

    #[test]
    fn test_weighted_ids() {
        let classifier = FastText::new_lid().expect("could not instantiate a classifier");
        let document = "This sentence is a long, long sentence that happens to be in english.
        This one too, what a coincidence, whew. The quick brown fox jumps over the lazy dog. This one too, what a coincidence, whew. The quick brown fox jumps over the lazy dog.
        Phrase courte en français
        il y en a 3 mais moins de contenu que les anglaises
        héhé c'est vrai que c'est petit
        qdlskfjqmfdjlmkj";

        let lines: Vec<&str> = document.lines().collect();
        let en_count = lines[0].len() + lines[1].len();
        let fr_count = lines[2].len() + lines[3].len() + lines[4].len();
        let unk_count = lines[5].len();
        let total_count = en_count + fr_count + unk_count;
        let (ids, langs, total_size) = classifier.get_weighted_ids(document.lines()).unwrap();

        println!("{:?}", ids);
        println!("{:?}", langs);
        println!("{:?}", total_size);
        // assert correct byte counts
        assert_eq!(langs.get(&Some(Lang::En)).unwrap().0, en_count);
        assert_eq!(langs.get(&Some(Lang::Fr)).unwrap().0, fr_count);
        assert_eq!(langs.get(&None).unwrap().0, unk_count);

        // assert correct language ids
        let expected_ids = [
            Some(Lang::En),
            Some(Lang::En),
            Some(Lang::Fr),
            Some(Lang::Fr),
            Some(Lang::Fr),
            None,
        ];

        assert_eq!(
            ids.into_iter()
                .map(|id| id.map(|x| *x.label()))
                .collect::<Vec<Option<Lang>>>(),
            expected_ids
        );

        // assert total count
        assert_eq!(total_count, total_size);

        //assert sum lengths
        let (lengths, _): (Vec<usize>, Vec<f32>) = langs.values().map(|v| (v.0, v.1)).unzip();
        assert_eq!(lengths.iter().sum::<usize>(), total_count);
    }
    // #[test]
    // fn test_clean_prediction_invalid() -> {

    // }
}
