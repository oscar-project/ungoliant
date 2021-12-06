/*! Annotates noisy content

Noisy content is content that has a too low letters/punctuation ratio.
 *  !*/

use unic_ucd::GeneralCategory;

use super::Annotate;
use crate::pipelines::oscardoc::types::Document;
pub struct Noisy {
    threshold: f64,
}

impl Default for Noisy {
    fn default() -> Self {
        Self { threshold: 0.5 }
    }
}
impl Annotate for Noisy {
    // fn annotate(&self, doc: &mut Document) {
    //     // TODO: use counters?
    //     let (letters, nonletters): (Vec<bool>, Vec<bool>) = doc
    //         .content()
    //         .chars()
    //         .map(|c| GeneralCategory::of(c).is_letter())
    //         .partition(|x| *x);

    //     let letters = letters.len();
    //     let nonletters = nonletters.len();
    //     let total_chars = (letters + nonletters) as f64;
    //     let threshold = self.threshold * total_chars;

    //     if nonletters > threshold.floor() as usize {
    //         doc.metadata_mut().set_annotation("noisy".to_string());
    //     }
    // }

    fn annotate(&self, doc: &mut Document) {
        // TODO: use counters?

        let nb_chars = doc.content().chars().count();
        let threshold = (nb_chars as f64 * self.threshold).floor() as usize;

        let letters = doc
            .content()
            .chars()
            .map(|c| GeneralCategory::of(c).is_letter());

        let mut nonletter_count = 0;
        let mut letter_count = 0;

        for i in letters {
            if !i {
                nonletter_count += 1;

                // if count is more than what we consider to be the threshold, stop there
                if nonletter_count > threshold {
                    doc.metadata_mut().set_annotation("noisy".to_string());
                    break;
                }
            } else {
                letter_count += 1;

                // same logic applies
                if letter_count > threshold {
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{
        pipelines::{oscardoc::types::Document, oscardoc::types::Metadata},
        transformers::Annotate,
    };

    use super::Noisy;

    #[test]
    fn test_full_noise() {
        let content = "/////////////////////////".to_string();
        let mut d = Document::new(content, HashMap::new(), Metadata::default());
        let a = Noisy::default();
        a.annotate(&mut d);

        assert!(d
            .metadata()
            .annotation()
            .unwrap()
            .contains(&"noisy".to_string()));
    }

    #[test]
    fn almost_full_noise() {
        let content = "/a//a////a////a/a//a////a///a/a//a/".to_string();
        let mut d = Document::new(content, HashMap::new(), Metadata::default());
        let a = Noisy::default();
        a.annotate(&mut d);

        assert!(d
            .metadata()
            .annotation()
            .unwrap()
            .contains(&"noisy".to_string()));
    }

    #[test]
    fn standard_punctuation() {
        let content = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. 
        Nam tempor magna ac justo sollicitudin, eu posuere purus sollicitudin. 
        Aliquam erat volutpat. 
        Duis dui ipsum, lacinia at ornare vitae, fringilla eu lorem. 
        Aenean nec justo neque. "
            .to_string();
        let mut d = Document::new(content, HashMap::new(), Metadata::default());
        let a = Noisy::default();
        a.annotate(&mut d);

        assert!(d.metadata().annotation().is_none())
    }
}
