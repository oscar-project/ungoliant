/*! Multilingual identification

This module can be used to check if a [Document] is multilingual.

In our case, multilingual documents are documents that have sentences in multiple languages in reasonable proportions, and of a reasonable quality.
For example, a document with 30 English sentences, 30 Spanish sentences and 30 French sentences is multilingual,
while a document having 99 English sentences and a unique French one is not.

There are currently two multilinguality implementations:

- [Multilingual] ranks language identifications and ensures that `C_n+1 >= (C_n)/Q`, with C_0 being the line or byte count for the most occurrent language, and Q a parameter.
- [StrictMultilingual] ensures that each present language has at least `C_tot/(n+1)` bytes or lines, and that the unidentified lines/bytes do not make more that `C_tot/(n+1)` bytes or lines.

There are other criteria that are specified in the structs docs.

!*/
use std::collections::HashMap;

use itertools::Itertools;
use log::debug;

use crate::filtering::Filter;

use super::identification::Identification;

/// Strict Multilingual detector
///
/// * `min_sentences`: Minimal number of total sentences
/// * `threshold_confidence`: Minimal prediction confidence for a given line
/// * `max_langs`: Maximum number of languages present in a single Document
/// * `min_confident_pctg`: Minimal percentage of lines having a `threshold_confidence` prediction confidence
pub struct StrictMultilingual {
    min_sentences: usize,
    threshold_confidence: f32,
    max_langs: Option<usize>,
    min_confident_pctg: f64,
}

impl Filter<&[(Option<Identification<String>>, usize)]> for StrictMultilingual {
    fn detect(&self, item: &[(Option<Identification<String>>, usize)]) -> bool {
        let nb_bytes: usize = item.iter().map(|(_, nb_bytes)| nb_bytes).sum();
        let nb_lines = item.len();

        // If there's not enough sentences, return false
        if item.len() < self.min_sentences {
            return false;
        }

        // get the number of lines that are confident enough
        let nb_confident = item
            .iter()
            .filter(|(id, _)| {
                if let Some(id) = id {
                    id.prob() >= &self.threshold_confidence
                } else {
                    false
                }
            })
            .count();

        // check if n% of the lines are confident enough
        if (nb_confident as f64 / nb_lines as f64) <= self.min_confident_pctg {
            return false;
        }

        let mut bytes_per_lang: HashMap<_, usize> = HashMap::new();
        bytes_per_lang.insert(None, 0);
        // count lines for each language AND for no-identification
        for (id, bytes) in item {
            // key is None for no identification
            let key = id.as_ref().map(|id| id.label());

            match bytes_per_lang.get_mut(&key) {
                Some(count) => *count += *bytes,
                None => {
                    bytes_per_lang.insert(key, *bytes);
                }
            }
        }

        let nb_langs = bytes_per_lang.keys().filter(|x| x.is_some()).count();
        // check if document is monolingual
        if nb_langs < 2 || nb_langs > self.max_langs.unwrap_or(usize::MAX) {
            return false;
        }

        let count_threshold =
            (nb_bytes as f32 / bytes_per_lang.keys().count() as f32).floor() as usize;
        for (lang, count) in bytes_per_lang {
            match lang {
                Some(_) => {
                    // if a provided language does not have enough sentences, return false
                    if count < count_threshold {
                        return false;
                    }
                }
                None => {
                    // if we got no-indentification sentences, ensure that we did not get too much of them
                    if count > count_threshold {
                        return false;
                    }
                }
            }
        }
        true
    }
}

impl Filter<&[Option<Identification<String>>]> for StrictMultilingual {
    fn detect(&self, item: &[Option<Identification<String>>]) -> bool {
        let nb_lines = item.len();
        // check if the document has less than 10 lines
        if item.len() < self.min_sentences {
            return false;
        }

        // get the number of lines that are confident enough
        let nb_confident = item
            .iter()
            .filter(|id| {
                if let Some(id) = id {
                    id.prob() >= &self.threshold_confidence
                } else {
                    false
                }
            })
            .count();

        // check if 90% of the lines are confident enough
        if (nb_confident as f64 / nb_lines as f64) <= self.min_confident_pctg {
            return false;
        }

        let mut sentences_per_lang = HashMap::new();
        // count lines for each language AND for no-identification
        for id in item {
            // key is None for no identification
            let key = id.as_ref().map(|id| id.label());

            let count = sentences_per_lang.entry(key).or_insert(0);
            *count += 1;
        }

        debug!("sentences per lang: {:?}", sentences_per_lang);
        let nb_langs = sentences_per_lang.keys().filter(|x| x.is_some()).count();

        // check if document is monolingual
        if nb_langs < 2 || nb_langs > self.max_langs.unwrap_or(usize::MAX) {
            return false;
        }

        debug!("candidate");
        // threshold is 1/nb_langs, with nb_langs including "unknown"
        let count_threshold =
            (nb_lines as f32 / sentences_per_lang.keys().count() as f32).floor() as i32;

        debug!("count_threshold is {}", count_threshold);
        for (lang, count) in sentences_per_lang {
            match lang {
                Some(lang) => {
                    // if a provided language does not have enough sentences, return false
                    if count < count_threshold {
                        debug!(
                            "{} has not enough sentences (has {}, must have {}",
                            lang, count, count_threshold
                        );
                        return false;
                    }
                }
                None => {
                    // if we got no-indentification sentences, ensure that we did not get too much of them
                    if count > count_threshold {
                        debug!(
                            "doc has too much unknown sentences (has {}, must have {})",
                            count, count_threshold
                        );
                        return false;
                    }
                }
            }
        }

        true
    }
}

impl Default for StrictMultilingual {
    fn default() -> Self {
        Self {
            min_sentences: 10,
            threshold_confidence: 0.8,
            min_confident_pctg: 0.8,
            max_langs: Some(5),
        }
    }
}

/// Less restrictive conditions for multilinguality.
///
/// * minimum of `10` sentences
/// * minimum of `2` languages
/// * When sorted, the number of sentences from the C_(n+1) >= C_(n) / Q (Q=4 by default)
///
/// # Example
///
/// If we have a 100 sentence document with 60 english lines, we'd need at least 60/4 = 15 lines in another language.
pub struct Multilingual {
    min_sentences: usize,
    limit: usize,
    q: f32,
}

impl Filter<&[Option<Identification<String>>]> for Multilingual {
    fn detect(&self, item: &[Option<Identification<String>>]) -> bool {
        if item.len() < self.min_sentences {
            return false;
        }
        // 2 langs minimum, the second one has at least 1/4 lines compared to the first one

        let mut sentences_per_lang = HashMap::new();
        // count lines for each language AND for no-identification
        for id in item {
            // key is None for no identification
            let key = id.as_ref().map(|id| id.label());

            let count = sentences_per_lang.entry(key).or_insert(0);
            *count += 1;
        }

        debug!("sentences per lang: {:?}", sentences_per_lang);
        let nb_langs = sentences_per_lang.keys().filter(|x| x.is_some()).count();

        // check if document is monolingual
        if nb_langs < 2 {
            debug!("not enough languages");
            return false;
        }

        // order by count
        let counts_ordered: Vec<_> = sentences_per_lang
            .into_iter()
            .sorted_unstable_by(|a, b| b.1.cmp(&a.1))
            .collect();

        // check that highest count is not None
        if let Some((None, _)) = counts_ordered.first() {
            debug!("first language is none");
            return false;
        }

        // take the n first (relevant) languages
        let mut l = counts_ordered
            .into_iter()
            .filter(|(lang, _)| lang.is_some())
            .take(self.limit);

        // first threshold is count for first language, divided by q
        let (first_lang, first_count) = l.next().unwrap();
        debug!("{:?} is first with {} lines", first_lang, first_count);
        let mut threshold = first_count as f32 / self.q;

        debug!("threshold is {}", threshold);
        // check that subsequent languages meet the criteria (C_n >= C_n-1 / q)
        // if that's the case, compute new threshold and continue
        for lang in l {
            debug!("testing {:?} for threshold", lang.0);
            if (lang.1 as f32) <= threshold {
                debug!(
                    "{:?}({}) does not meet the threshold {}",
                    lang.0, lang.1, threshold
                );
                return false;
            }

            debug!(
                "{:?}({}) does meet the threshold {}",
                lang.0, lang.1, threshold
            );
            threshold = lang.1 as f32 / self.q;
        }

        true
    }
}

impl Default for Multilingual {
    fn default() -> Self {
        Self {
            min_sentences: 10,
            limit: 2,
            q: 4.0,
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        filtering::Filter,
        identifiers::{
            identification::Identification, multilingual::Multilingual, StrictMultilingual,
        },
    };
    use lazy_static::lazy_static;
    use oxilangtag::LanguageTag;

    lazy_static! {
        pub static ref ID_EN: LanguageTag<String> = LanguageTag::parse("en".to_string()).unwrap();
        pub static ref ID_FR: LanguageTag<String> = LanguageTag::parse("fr".to_string()).unwrap();
    }

    #[test]
    fn test_multilingual() {
        let id = Some(Identification::new(ID_EN.clone(), 1.0));
        let ids = vec![id; 10];
        let m = Multilingual::default();
        assert_eq!(m.detect(&ids), false);
    }

    #[test]
    fn test_multilingual2() {
        let id = [
            Some(Identification::new(ID_EN.clone(), 1.0)),
            Some(Identification::new(ID_EN.clone(), 1.0)),
            Some(Identification::new(ID_FR.clone(), 1.0)),
            Some(Identification::new(ID_FR.clone(), 1.0)),
        ]
        .into_iter()
        .cycle();
        let ids: Vec<_> = id.take(20).collect();
        let m = Multilingual::default();
        assert_eq!(m.detect(&ids), true);
    }

    #[test]
    fn strict_bytes_false() {
        let id = [
            (Some(Identification::new(ID_EN.clone(), 1.0)), 100),
            (Some(Identification::new(ID_EN.clone(), 1.0)), 100),
            (Some(Identification::new(ID_FR.clone(), 1.0)), 1),
            (Some(Identification::new(ID_FR.clone(), 1.0)), 10),
            (Some(Identification::new(ID_FR.clone(), 1.0)), 10),
            (Some(Identification::new(ID_FR.clone(), 1.0)), 10),
        ]
        .into_iter()
        .cycle();
        let ids: Vec<(_, usize)> = id.take(20).collect();
        let m = StrictMultilingual::default();
        assert_eq!(m.detect(&ids[..]), false);
    }

    #[test]
    fn strict_bytes_true() {
        let id = [
            (Some(Identification::new(ID_EN.clone(), 1.0)), 100),
            (Some(Identification::new(ID_EN.clone(), 1.0)), 110),
            (Some(Identification::new(ID_EN.clone(), 1.0)), 111),
            (Some(Identification::new(ID_FR.clone(), 1.0)), 100),
            (Some(Identification::new(ID_FR.clone(), 1.0)), 130),
            (Some(Identification::new(ID_FR.clone(), 1.0)), 10),
        ]
        .into_iter()
        .cycle();
        let ids: Vec<(_, usize)> = id.take(20).collect();
        let m = StrictMultilingual::default();
        assert_eq!(m.detect(&ids[..]), true);
    }

    // test strict multilinguality
    // Ensure to have enough (>1/3 of total) in two langs, with a little junk data
    #[test]
    fn strict_bytes_with_junk() {
        let id = [
            (Some(Identification::new(ID_EN.clone(), 1.0)), 100),
            (Some(Identification::new(ID_EN.clone(), 1.0)), 110),
            (Some(Identification::new(ID_EN.clone(), 1.0)), 111),
            (Some(Identification::new(ID_EN.clone(), 1.0)), 45),
            (None, 100),
            (None, 150),
            // (None, 3),
            (Some(Identification::new(ID_FR.clone(), 1.0)), 100),
            (Some(Identification::new(ID_FR.clone(), 1.0)), 130),
            (Some(Identification::new(ID_FR.clone(), 1.0)), 130),
            (Some(Identification::new(ID_FR.clone(), 1.0)), 130),
            (Some(Identification::new(ID_FR.clone(), 1.0)), 10),
        ]
        .into_iter()
        .cycle();
        let ids: Vec<(_, usize)> = id.take(200).collect();
        let m = StrictMultilingual::default();
        let ret = m.detect(&ids[..]);
        assert_eq!(ret, true);
    }
    // test strict multilinguality
    // Ensure to have enough (>1/3 of total) in two langs, with no junk data
    #[test]
    fn strict_bytes_no_junk() {
        let id = [
            (Some(Identification::new(ID_EN.clone(), 1.0)), 100),
            (Some(Identification::new(ID_EN.clone(), 1.0)), 110),
            (Some(Identification::new(ID_EN.clone(), 1.0)), 111),
            (Some(Identification::new(ID_EN.clone(), 1.0)), 45),
            (Some(Identification::new(ID_FR.clone(), 1.0)), 100),
            (Some(Identification::new(ID_FR.clone(), 1.0)), 130),
            (Some(Identification::new(ID_FR.clone(), 1.0)), 130),
            (Some(Identification::new(ID_FR.clone(), 1.0)), 130),
            (Some(Identification::new(ID_FR.clone(), 1.0)), 10),
        ]
        .into_iter()
        .cycle();
        let ids: Vec<(_, usize)> = id.take(200).collect();
        let m = StrictMultilingual::default();
        let ret = m.detect(&ids[..]);
        assert_eq!(ret, true);
    }
    #[test]
    fn test_too_short() {
        let id = [(Some(Identification::new(ID_EN.clone(), 1.0)), 100)]
            .into_iter()
            .cycle();

        let ids: Vec<(_, usize)> = id.take(2).collect();
        let m = StrictMultilingual::default();
        assert_eq!(m.detect(&ids[..]), false);
    }

    #[test]
    fn test_not_confident_enough() {
        let id = [(Some(Identification::new(ID_EN.clone(), 0.1)), 100)]
            .into_iter()
            .cycle();

        let ids: Vec<(_, usize)> = id.take(2).collect();
        let m = StrictMultilingual::default();
        assert_eq!(m.detect(&ids[..]), false);
    }

    #[test]
    fn test_too_much_none() {
        let id = [
            (Some(Identification::new(ID_EN.clone(), 0.1)), 100),
            (None, 100),
            (None, 100),
            (None, 100),
        ]
        .into_iter()
        .cycle();

        let ids: Vec<(_, usize)> = id.take(2).collect();
        let m = StrictMultilingual::default();
        assert_eq!(m.detect(&ids[..]), false);
    }

    #[test]
    fn test_too_much_languages() {
        let id = [
            (Some(Identification::new(ID_EN.clone(), 0.1)), 100),
            (Some(Identification::new(ID_FR.clone(), 0.1)), 100),
            (
                Some(Identification::new(
                    LanguageTag::parse("uk".to_string()).unwrap(),
                    0.1,
                )),
                100,
            ),
            (
                Some(Identification::new(
                    LanguageTag::parse("fi".to_string()).unwrap(),
                    0.1,
                )),
                100,
            ),
            (
                Some(Identification::new(
                    LanguageTag::parse("uz".to_string()).unwrap(),
                    0.1,
                )),
                100,
            ),
            (
                Some(Identification::new(
                    LanguageTag::parse("pa".to_string()).unwrap(),
                    0.1,
                )),
                100,
            ),
            (
                Some(Identification::new(
                    LanguageTag::parse("zh".to_string()).unwrap(),
                    0.1,
                )),
                100,
            ),
        ]
        .into_iter()
        .cycle();

        let ids: Vec<(_, usize)> = id.take(10).collect();
        let m = StrictMultilingual::default();
        assert_eq!(m.detect(&ids[..]), false);
    }
    #[test]
    fn test_too_little_languages() {
        let id = [(Some(Identification::new(ID_EN.clone(), 0.1)), 100)]
            .into_iter()
            .cycle();

        let ids: Vec<(_, usize)> = id.take(2).collect();
        let m = StrictMultilingual::default();
        assert_eq!(m.detect(&ids[..]), false);
    }
}
