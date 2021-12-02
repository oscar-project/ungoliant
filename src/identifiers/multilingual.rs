use std::{collections::HashMap, ops::Mul};

use itertools::Itertools;
use log::{debug, info};

use crate::filtering::Filter;

use super::Identification;

pub struct StrictMultilingual {
    min_sentences: usize,
    threshold_confidence: f32,
    max_langs: Option<usize>,
    min_confident_pctg: f64,
}

impl Filter<&[(Option<Identification>, usize)]> for StrictMultilingual {
    fn detect(&self, item: &[(Option<Identification>, usize)]) -> bool {
        let nb_bytes: usize = item.iter().map(|(_, nb_bytes)| nb_bytes).sum();
        let nb_lines = item.len();

        if item.len() < self.min_sentences {
            return false;
        }

        // get the number of lines that are confident enough
        let nb_confident = item
            .iter()
            .filter(|(id, _)| {
                if let Some(id) = id {
                    if id.prob() >= &self.threshold_confidence {
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            })
            .count();

        // check if 90% of the lines are confident enough
        if (nb_confident as f64 / nb_lines as f64) <= self.min_confident_pctg {
            return false;
        }

        let mut bytes_per_lang = HashMap::new();
        // count lines for each language AND for no-identification
        for (id, bytes) in item {
            // key is None for no identification
            let key = if let Some(id) = id {
                Some(*id.label())
            } else {
                None
            };

            let count = bytes_per_lang.entry(key).or_insert(*bytes);
            *count += *bytes;
        }

        println!("bytes per lang: {:?}", bytes_per_lang);
        let nb_langs = bytes_per_lang.keys().filter(|x| x.is_some()).count();
        // check if document is monolingual
        if nb_langs < 2 || nb_langs > self.max_langs.unwrap_or(usize::MAX) {
            return false;
        }

        let count_threshold =
            (nb_bytes as f32 / bytes_per_lang.keys().count() as f32).floor() as usize;

        println!("count_threshold is {}", count_threshold);
        for (lang, count) in bytes_per_lang {
            match lang {
                Some(lang) => {
                    // if a provided language does not have enough sentences, return false
                    if count < count_threshold {
                        println!(
                            "{} has not enough sentences (has {}, must have {}",
                            lang, count, count_threshold
                        );
                        return false;
                    }
                }
                None => {
                    // if we got no-indentification sentences, ensure that we did not get too much of them
                    if count > count_threshold {
                        println!(
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

impl Filter<&[Option<Identification>]> for StrictMultilingual {
    fn detect(&self, item: &[Option<Identification>]) -> bool {
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
                    if id.prob() >= &self.threshold_confidence {
                        true
                    } else {
                        false
                    }
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
            let key = if let Some(id) = id {
                Some(*id.label())
            } else {
                None
            };

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

impl Filter<&[Option<Identification>]> for Multilingual {
    fn detect(&self, item: &[Option<Identification>]) -> bool {
        if item.len() < self.min_sentences {
            return false;
        }
        // 2 langs minimum, the second one has at least 1/4 lines compared to the first one

        let mut sentences_per_lang = HashMap::new();
        // count lines for each language AND for no-identification
        for id in item {
            // key is None for no identification
            let key = if let Some(id) = id {
                Some(*id.label())
            } else {
                None
            };

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
        identifiers::{multilingual::Multilingual, Identification, StrictMultilingual},
        lang::Lang,
    };

    #[test]
    fn test_multilingual() {
        let id = Some(Identification::new(Lang::En, 1.0));
        let ids = vec![id.clone(); 10];
        let m = Multilingual::default();
        assert_eq!(m.detect(&ids), false);
    }

    #[test]
    fn test_multilingual2() {
        let id = [
            Some(Identification::new(Lang::En, 1.0)),
            Some(Identification::new(Lang::En, 1.0)),
            Some(Identification::new(Lang::Fr, 1.0)),
            Some(Identification::new(Lang::Fr, 1.0)),
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
            (Some(Identification::new(Lang::En, 1.0)), 100),
            (Some(Identification::new(Lang::En, 1.0)), 100),
            (Some(Identification::new(Lang::Fr, 1.0)), 1),
            (Some(Identification::new(Lang::Fr, 1.0)), 10),
            (Some(Identification::new(Lang::Fr, 1.0)), 10),
            (Some(Identification::new(Lang::Fr, 1.0)), 10),
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
            (Some(Identification::new(Lang::En, 1.0)), 100),
            (Some(Identification::new(Lang::En, 1.0)), 100),
            (Some(Identification::new(Lang::Fr, 1.0)), 100),
            (Some(Identification::new(Lang::Fr, 1.0)), 100),
            (Some(Identification::new(Lang::Fr, 1.0)), 10),
        ]
        .into_iter()
        .cycle();
        let ids: Vec<(_, usize)> = id.take(20).collect();
        let m = StrictMultilingual::default();
        assert_eq!(m.detect(&ids[..]), true);
    }
}
