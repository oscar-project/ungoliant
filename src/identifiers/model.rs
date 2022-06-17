/*! New-style FastText model.
   Uses [oxilangtag::LanguageTag] rather than Lang.
* !*/
use std::{
    collections::{HashMap, HashSet},
    marker::PhantomData,
    ops::Deref,
    path::Path,
    str::Lines,
};

use fasttext::FastText as FastTextLib;
use log::error;
use oxilangtag::LanguageTag;

use crate::{error::Error, identifiers::tag_convert::OldTag, lang::Lang};

use super::{identification::Identification, tag_convert::NewTag, NEW_LANGS, OLD_LANGS};

/// Covers individual sentence identifications, lang bins and total size of document in bytes
#[derive(Debug)]
pub struct DocIdentification<T: Deref<Target = str> + Clone> {
    line_ids: Vec<Option<Identification<T>>>,
    lang_bins: HashMap<Option<LanguageTag<T>>, (usize, f32)>,
    total_size: usize,
}

impl<T: Deref<Target = str> + Clone> DocIdentification<T> {
    pub fn line_ids(&self) -> &[Option<Identification<T>>] {
        self.line_ids.as_ref()
    }

    pub fn lang_bins(&self) -> &HashMap<Option<LanguageTag<T>>, (usize, f32)> {
        &self.lang_bins
    }

    pub fn total_size(&self) -> usize {
        self.total_size
    }
}

pub trait ModelKind {
    fn labels() -> &'static HashSet<LanguageTag<String>>;
}
pub struct Old;
impl ModelKind for Old {
    fn labels() -> &'static HashSet<LanguageTag<String>> {
        &OLD_LANGS
    }
}
pub struct New;
impl ModelKind for New {
    fn labels() -> &'static HashSet<LanguageTag<String>> {
        &NEW_LANGS
    }
}

/// Prediction trait.
///
/// Enables prediction on a single line (top-1 and top-k) and on a set of lines.
pub trait Predict<T: Deref<Target = str> + Clone> {
    fn predict_one(&self, line: &str) -> Result<Option<Identification<T>>, Error>;
    fn predict(&self, line: &str) -> Result<Option<Vec<Identification<T>>>, Error>;
    fn weighted_ids(&self, lines: Lines) -> Result<DocIdentification<T>, Error>;
}

/// FastTextModel.
///
/// ModelKind will condition the implementation of the tag conversion
pub struct FastText<T: ModelKind> {
    inner: FastTextLib,
    fasttext_kind: PhantomData<T>,
    pub k: i32,
    pub threshold: f32,
}

impl<T: ModelKind> FastText<T> {
    /// removes __label__ from identification start
    fn clean_label(label: &str) -> String {
        label[..9].to_string()
    }
}

/// Prediction for old tags.
impl Predict<String> for FastText<Old> {
    fn predict_one(&self, line: &str) -> Result<Option<Identification<String>>, Error> {
        let pred = self.inner.predict(line, 1, self.threshold)?;
        if pred.is_empty() {
            Ok(None)
        } else {
            // unwrapping because we know pred is not empty.
            // We might have a better way of doing this.
            // The idea is to move out of pred, since we won't need it afterwards.
            let pred = pred.into_iter().next().unwrap();

            // convert prediction to newtag
            let pred_to_languagetag: Result<LanguageTag<String>, _> = OldTag(pred.label).try_into();
            match pred_to_languagetag {
                Ok(label) => {
                    let id = Identification::new(label, pred.prob);

                    Ok(Some(id))
                }
                Err(e) => {
                    error!("Couldn't find a proper label: {e:?}");
                    Err(e.into())
                }
            }
        }
    }

    fn predict(&self, line: &str) -> Result<Option<Vec<Identification<String>>>, Error> {
        let predictions = self.inner.predict(line, self.k, self.threshold)?;
        if predictions.is_empty() {
            Ok(None)
        } else {
            let identifications: Vec<Identification<String>> = predictions
                .into_iter()
                //TODO: try_into coerces into OldTag?
                .map(|pred| {
                    let label: Result<LanguageTag<String>, _> = OldTag(pred.label).try_into();
                    match label {
                        Ok(l) => Ok(Identification::new(l, pred.prob)),
                        Err(e) => Err(e),
                    }
                })
                .filter_map(|pred_result| match pred_result {
                    Ok(p) => Some(p),
                    Err(e) => {
                        error!("Error with tag: {e}");
                        None
                    }
                })
                .collect();
            //do new stuff
            Ok(Some(identifications))
        }
    }

    fn weighted_ids(&self, lines: Lines) -> Result<DocIdentification<String>, Error> {
        // per-lang and total byte counts
        // lang_count maps Lang -> (lang_byte_count, sum(byte_count*prob))
        let mut lang_count = HashMap::new();
        let mut total_count = 0;

        // filter out unicode null chars
        // this prevents fasttext errors and hopefully improves
        // corpus quality
        // TODO: check if we need this line
        let lines = lines.map(|l| l.replace(char::from(0), ""));

        let ids: Vec<Option<Identification<_>>> = lines
            .map(|line| {
                // identify
                let id = self.predict_one(&line);

                // add to byte count for document-level identification
                if let Ok(ref ide) = id {
                    // map Identification to its lang, or keep None to store the "None" language identification
                    let ide_label = ide.as_ref().map(|i| i.label().clone());
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

        Ok(DocIdentification {
            line_ids: ids,
            lang_bins: lang_count,
            total_size: total_count,
        })
    }
}

/// Prediction for new tags/model
impl Predict<String> for FastText<New> {
    fn predict_one(&self, line: &str) -> Result<Option<Identification<String>>, Error> {
        let pred = self.inner.predict(line, 1, self.threshold)?;
        if pred.is_empty() {
            Ok(None)
        } else {
            // unwrapping because we know pred is not empty.
            // We might have a better way of doing this.
            // The idea is to move out of pred, since we won't need it afterwards.
            let pred = pred.into_iter().next().unwrap();

            // convert prediction to newtag
            let pred_to_languagetag: Result<LanguageTag<String>, _> = NewTag(pred.label).try_into();
            match pred_to_languagetag {
                Ok(label) => {
                    let id = Identification::new(label, pred.prob);

                    Ok(Some(id))
                }
                Err(e) => {
                    error!("Couldn't find a proper label: {e:?}");
                    Err(e.into())
                }
            }
        }
    }

    fn predict(&self, line: &str) -> Result<Option<Vec<Identification<String>>>, Error> {
        let predictions = self.inner.predict(line, self.k, self.threshold)?;
        if predictions.is_empty() {
            Ok(None)
        } else {
            let identifications: Vec<Identification<String>> = predictions
                .into_iter()
                //TODO: try_into coerces into OldTag?
                .map(|pred| {
                    let label: Result<LanguageTag<String>, _> = NewTag(pred.label).try_into();
                    match label {
                        Ok(l) => Ok(Identification::new(l, pred.prob)),
                        Err(e) => Err(e),
                    }
                })
                .filter_map(|pred_result| match pred_result {
                    Ok(p) => Some(p),
                    Err(e) => {
                        error!("Error with tag: {e}");
                        None
                    }
                })
                .collect();
            //do new stuff
            Ok(Some(identifications))
        }
    }

    fn weighted_ids(&self, lines: Lines) -> Result<DocIdentification<String>, Error> {
        // per-lang and total byte counts
        // lang_count maps Lang -> (lang_byte_count, sum(byte_count*prob))
        let mut lang_count = HashMap::new();
        let mut total_count = 0;

        // filter out unicode null chars
        // this prevents fasttext errors and hopefully improves
        // corpus quality
        // TODO: check if we need this line
        let lines = lines.map(|l| l.replace(char::from(0), ""));

        let ids: Vec<Option<Identification<_>>> = lines
            .map(|line| {
                // identify
                let id = self.predict_one(&line);

                // add to byte count for document-level identification
                if let Ok(ref ide) = id {
                    // map Identification to its lang, or keep None to store the "None" language identification
                    let ide_label = ide.as_ref().map(|i| i.label().clone());
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

        Ok(DocIdentification {
            line_ids: ids,
            lang_bins: lang_count,
            total_size: total_count,
        })
    }
}

/// Fasttext builder.
pub struct FastTextBuilder<'a, T> {
    path: Option<&'a Path>,
    kind: PhantomData<T>,
    k: Option<i32>,
    threshold: Option<f32>,
}

impl<'a, T: ModelKind> FastTextBuilder<'a, T> {
    fn init_fasttextlib(path: &str) -> Result<fasttext::FastText, Error> {
        let mut ft = FastTextLib::new();
        ft.load_model(path)?;
        Ok(ft)
    }
    /// attempt to build, resort to the following defaults if not set:
    /// - path: "./lid.208a.bin"
    /// - k: 1
    /// threshold: 0.8
    pub fn build_or_default(&self) -> Result<FastText<T>, Error> {
        let inner = {
            let path = match self.path {
                Some(p) => p
                    .to_str()
                    .ok_or(Error::Custom("Could not parse path.".to_string()))?,
                None => "lid.208a.bin",
            };
            Self::init_fasttextlib(path)?
        };

        let k = self.k.unwrap_or(1);
        let threshold = self.threshold.unwrap_or(0.8);

        Ok(FastText {
            inner,
            fasttext_kind: PhantomData,
            k,
            threshold,
        })
    }

    pub fn build(&self) -> Result<FastText<T>, Error> {
        let error = if self.path == None {
            Some("No path provided")
        } else if self.k == None {
            Some("No k provided")
        } else if self.threshold == None {
            Some("No threshold provided")
        } else {
            None
        };

        if let Some(e) = error {
            return Err(Error::Custom(e.to_string()));
        }

        let path = self
            .path
            .unwrap()
            .to_str()
            .ok_or(Error::Custom("Couldn't parse path".to_string()))?;
        Ok(FastText {
            inner: Self::init_fasttextlib(path)?,
            fasttext_kind: PhantomData,
            k: self.k.unwrap(),
            threshold: self.threshold.unwrap(),
        })
    }
    pub fn path<'b>(&'b mut self, path: &'a Path) -> &'b mut FastTextBuilder<'a, T> {
        self.path = Some(path);
        self
    }

    pub fn k<'b>(&'b mut self, k: i32) -> &'b mut FastTextBuilder<'a, T> {
        self.k = Some(k);
        self
    }

    pub fn threshold<'b>(&'b mut self, threshold: f32) -> &'b mut FastTextBuilder<'a, T> {
        self.threshold = Some(threshold);
        self
    }
}

impl<'a, T> Default for FastTextBuilder<'a, T> {
    fn default() -> Self {
        Self {
            path: Default::default(),
            kind: Default::default(),
            k: Default::default(),
            threshold: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{FastText, FastTextBuilder, New, Predict};
    use language_tags::LanguageTag;

    #[test]
    fn test_new_one_sentence() {
        let model: FastText<New> = FastTextBuilder::default()
            .path(Path::new("lid.218a.bin"))
            .build_or_default()
            .unwrap();

        let sentence = "Ceci est une phrase en Français :)";

        let pred = model.predict_one(sentence);

        println!("{pred:?}");
    }

    #[test]
    fn test_new_document() {
        let document = "Ceci est une phrase en français.
        This is a sentence in english. 
        Ola, como estas? Esta pregunta estas en español!"
            .lines();

        let model: FastText<New> = FastTextBuilder::default()
            .path(Path::new("lid.218a.bin"))
            .build_or_default()
            .unwrap();

        let pred = model.weighted_ids(document);
        println!("{pred:#?}");
    }

    #[test]
    fn test_old_one_sentence() {
        let model: FastText<New> = FastTextBuilder::default()
            .path(Path::new("lid.176.bin"))
            .build_or_default()
            .unwrap();

        let sentence = "Ceci est une phrase en Français :)";

        let pred = model.predict_one(sentence).unwrap();

        println!("{pred:?}");
    }

    #[test]
    fn test_old_and_new_coherence() {
        let old_model: FastText<New> = FastTextBuilder::default()
            .path(Path::new("lid.176.bin"))
            .build_or_default()
            .unwrap();
        let new_model: FastText<New> = FastTextBuilder::default()
            .path(Path::new("lid.218a.bin"))
            .build_or_default()
            .unwrap();

        let sentence = "Salut, je suis une phrase française :)";

        let old_pred = old_model.predict_one(sentence).unwrap();
        let new_pred = new_model.predict_one(sentence).unwrap();

        // let old_pred_canon = LanguageTag::parse(old_pred.unwrap().label().as_str())
        //     .unwrap()
        //     .canonicalize()
        //     .unwrap();
        // let new_pred_canon = LanguageTag::parse(new_pred.unwrap().label().as_str())
        //     .unwrap()
        //     .canonicalize()
        //     .unwrap();

        // let zh_6391 = LanguageTag::parse("zh").unwrap();
        // let zh_6392 = LanguageTag::parse("zho").unwrap().canonicalize().unwrap();
        // assert_eq!(zh_6391, zh_6392);
        // println!("{new_pred_canon}, {old_pred_canon}");
        assert_eq!(old_pred.unwrap().label(), new_pred.unwrap().label());
    }
}
