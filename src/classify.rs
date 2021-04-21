use fasttext::{FastText, Prediction};

const MIN_SENTENCE_LEN: usize = 100;

/// changes the label field from `__label__xx` into `xx`
fn clean_prediction(prediction: Prediction) -> Result<Prediction, String> {
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

/// ensure that sentences meet valid requirements
/// to be sent to fasttext:
/// - valid utf8: currently handled upper in the chain because strings can't be invalid utf8
/// - > 100 chars (go runes)
/// However, we're currently using from_utf8_lossy.
/// We have to use from_utf8 and catch failing strings
///
/// We also use chars(), that gives Unicode scalar values, not graphemes.
fn valid(sentence: &str) -> bool {
    // no checking in utf8 validity since 8
    sentence.chars().count() > MIN_SENTENCE_LEN
}

/// A [fasttext::FastText] instance.
/// Should be replaced for a more generic struct allowing different
/// predictors.
pub struct Classifier {
    predictor: FastText,
    pub k: i32,
    pub threshold: f32,
}

impl Classifier {
    /// Create a new fasttext classifier allowing to identify
    /// language of strings.
    ///
    /// *Having `lid.176.bin` at `.` is mandatory*
    ///
    /// # Errors
    /// Propagates [fasttext::FastText] errors.
    pub fn new_lid() -> Result<Self, String> {
        Self::new("lid.176.bin", 1, 0.8)
    }

    /// Create a new fasttext classifier.
    ///
    /// filename has to be a path to a `bin` file.
    ///
    /// See [fasttext::FastText::predict] for other parameters explanation
    pub fn new(filename: &str, k: i32, threshold: f32) -> Result<Self, String> {
        let mut predictor = FastText::new();
        predictor.load_model(filename)?;
        Ok(Classifier {
            predictor,
            k,
            threshold,
        })
    }

    /// predict for supplied sentence.
    /// returned [Vec] is empty if no prediction passes threshold or if sentence
    /// does not pass [valid].
    pub fn predict(&self, sentence: &str) -> Result<Vec<Prediction>, String> {
        if !valid(sentence) {
            return Ok(vec![]);
        }
        let predictions = self.predictor.predict(sentence, self.k, self.threshold)?;

        Ok(predictions
            .into_iter()
            .map(|p| clean_prediction(p).unwrap())
            .collect())
    }
}
