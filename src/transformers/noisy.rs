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
    fn annotate(&self, doc: &mut Document) {
        // TODO: use counters?

        let nb_chars = doc.content().chars().count();
        let threshold = (nb_chars as f64 * self.threshold).floor() as usize;

        let letters = doc.content().chars().map(|c| {
            let gc = GeneralCategory::of(c);

            gc.is_letter() || gc.is_mark()
        });

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

    // #[test]
    // fn test_full_noise() {
    //     let content = "/////////////////////////".to_string();
    //     let mut d = Document::new(content, HashMap::new(), Metadata::default());
    //     let a = Noisy::default();
    //     a.annotate(&mut d);

    //     assert!(d
    //         .metadata()
    //         .annotation()
    //         .unwrap()
    //         .contains(&"noisy".to_string()));
    // }

    // #[test]
    // fn almost_full_noise() {
    //     let content = "/a//a////a////a/a//a////a///a/a//a/".to_string();
    //     let mut d = Document::new(content, HashMap::new(), Metadata::default());
    //     let a = Noisy::default();
    //     a.annotate(&mut d);

    //     assert!(d
    //         .metadata()
    //         .annotation()
    //         .unwrap()
    //         .contains(&"noisy".to_string()));
    // }

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

    #[test]
    fn script() {
        let content = "ക്ഷീണിച്ചു തളര്‍ന്നു ചുറ്റിലുമുള്ള ലോകത്തിന്റെ ഔപചാരിതകളെയെല്ലാം 
        കവച്ചു വെച്ച് കിടന്നുറങ്ങുന്ന ജമാലിനെ മിക്കപ്പോഴും ഈ ഉച്ചയുറക്കത്തില്‍ നിന്നും 
        വിളിച്ചുണര്‍ത്തിയിരുന്നത് ചെരിഞ്ഞു പറക്കുന്ന ഒരു വിമാനചിത്രം പതിച്ച ഇളംനീല 
        എയര്‍മെയിലായിരുന്നു. ഗ്രീഷ്മകാല നട്ടുച്ചയുടെ ചുട്ടുപൊള്ളുന്ന വെയില്‍ മുഴുവന്‍ 
        ഏറ്റുവാങ്ങിത്തളര്‍ന്ന ശരീരം പതിവുപോലെ പിന്നെയും പിന്നെയും 
        അബോധത്തിലേക്ക്‌ നൂണ്ടു പോവാന്‍ നിര്‍ബ്ബ‍ന്ധിച്ചിട്ടും അതനുസരിക്കാതെ 
        വന്നുമൂടുന്ന നിദ്രയെ തല കുടഞ്ഞെറിഞ്ഞ് അന്നും അയാള്‍ പെട്ടെന്ന് തന്നെ 
        കട്ടിലില്‍ എഴുനേറ്റിരുന്നു. തലേന്ന് രാത്രി നേരമേറെ വൈകി തന്റെ തൂവിപ്പോയ 
        ദൈന്യങ്ങള്‍ അരിച്ചെടുത്തും സ്നേഹമിടിപ്പുകളു
        "
        .to_string();

        let mut d = Document::new(content, HashMap::new(), Metadata::default());
        let a = Noisy::default();
        a.annotate(&mut d);

        assert!(d.metadata().annotation().is_none())
    }
}
