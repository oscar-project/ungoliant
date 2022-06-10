//! Conversion utilities or fasttext tags to standardized BCP47.
use std::collections::HashMap;
use std::hash::Hash;

// use language_tags::{LanguageTag, ParseError};
use lazy_static::lazy_static;
use oxilangtag::{LanguageTag, LanguageTagParseError};
use std::ops::Deref;

lazy_static! {
    pub static ref NEW_TAG_REPLACE: HashMap<&'static str, &'static str> = [
        ("abk", "ab"),
        ("ace_Arab", "ace-Arab"),
        ("ace_Latn", "ace-Latn"),
        ("afr", "af"),
        ("aka", "ak"),
        ("amh", "am"),
        ("ara_Arab", "ar-Arab"),
        ("ara_Latn", "ar-Latn"),
        ("asm", "as"),
        ("bak", "ba"),
        ("bam", "bm"),
        ("bel", "be"),
        ("ben", "bn"),
        ("bis", "bi"),
        ("bjn_Arab", "bjn-Arab"),
        ("bjn_Latn", "bjn-Latn"),
        ("bod", "bo"),
        ("bos", "bs"),
        ("bul", "bg"),
        ("cat", "ca"),
        ("ces", "cs"),
        ("che", "ce"),
        ("chv", "cv"),
        ("crh_Latn", "crh-Latn"),
        ("cym", "cy"),
        ("dan", "da"),
        ("deu", "de"),
        ("dzo", "dz"),
        ("ell", "el"),
        ("eng", "en"),
        ("epo", "eo"),
        ("est", "et"),
        ("eus", "eu"),
        ("ewe", "ee"),
        ("fao", "fo"),
        ("fas", "fa"),
        ("fij", "fj"),
        ("fin", "fi"),
        ("fra", "fr"),
        ("gla", "gd"),
        ("gle", "ga"),
        ("glg", "gl"),
        ("grn", "gn"),
        ("guj", "gu"),
        ("hat", "ht"),
        ("hau", "ha"),
        ("heb", "he"),
        ("hin", "hi"),
        ("hrv", "hr"),
        ("hun", "hu"),
        ("hye", "hy"),
        ("ibo", "ig"),
        ("ind", "id"),
        ("isl", "is"),
        ("ita", "it"),
        ("jav", "jv"),
        ("jpn", "ja"),
        ("kal", "kl"),
        ("kan", "kn"),
        ("kas_Arab", "ks-Arab"),
        ("kas_Deva", "ks-Deva"),
        ("kat", "ka"),
        ("kau_Arab", "kr-Arab"),
        ("kau_Latn", "kr-Latn"),
        ("kaz", "kk"),
        ("khm", "km"),
        ("kik", "ki"),
        ("kin", "rw"),
        ("kir", "ky"),
        ("kon", "kg"),
        ("kor", "ko"),
        ("kur", "ku"),
        ("lao", "lo"),
        ("lav", "lv"),
        ("lim", "li"),
        ("lin", "ln"),
        ("lit", "lt"),
        ("ltz", "lb"),
        ("lug", "lg"),
        ("mal", "ml"),
        ("mar", "mr"),
        ("min_Latn", "min-Latn"),
        ("mkd", "mk"),
        ("mlg", "mg"),
        ("mlt", "mt"),
        ("mni_Mtei", "mni-Mtei"),
        ("mon", "mn"),
        ("mri", "mi"),
        ("msa", "ms"),
        ("mya", "my"),
        ("nav", "nv"),
        ("nld", "nl"),
        ("nno", "nn"),
        ("nob", "nb"),
        ("nya", "ny"),
        ("oci", "oc"),
        ("orm", "om"),
        ("oss", "os"),
        ("pan", "pa"),
        ("pol", "pl"),
        ("por", "pt"),
        ("prs", "fa-AF"),
        ("pus", "ps"),
        ("que", "qu"),
        ("roh", "rm"),
        ("ron", "ro"),
        ("run", "rn"),
        ("rus", "ru"),
        ("sag", "sg"),
        ("san", "sa"),
        ("sin", "si"),
        ("slk", "sk"),
        ("slv", "sl"),
        ("smo", "sm"),
        ("sna", "sn"),
        ("snd", "sd"),
        ("som", "so"),
        ("sot", "st"),
        ("spa", "es"),
        ("sqi", "sq"),
        ("srd", "sc"),
        ("srp_Cyrl", "sr-Cyrl"),
        ("ssw", "ss"),
        ("sun", "su"),
        ("swe", "sv"),
        ("tah", "ty"),
        ("tam", "ta"),
        ("tat_Cyrl", "tt-Cyrl"),
        ("tel", "te"),
        ("tgk", "tg"),
        ("tgl", "fil"),
        ("tha", "th"),
        ("tir", "ti"),
        ("tmh_Latn", "tmh-Latn"),
        ("tmh_Tfng", "tmh-Tfng"),
        ("ton", "to"),
        ("tsn", "tn"),
        ("tso", "ts"),
        ("tuk", "tk"),
        ("tur", "tr"),
        ("twi", "tw"),
        ("uig", "ug"),
        ("ukr", "uk"),
        ("urd", "ur"),
        ("uzb", "uz"),
        ("vie", "vi"),
        ("wol", "wo"),
        ("xho", "xh"),
        ("yid", "yi"),
        ("yor", "yo"),
        ("zho_Hans", "zh-Hans"),
        ("zho_Hant", "zh-Hant"),
        ("zul", "zu"),
    ]
    .into_iter()
    .collect();
}

pub enum Tag<T: Deref<Target = str> + Clone> {
    Old(OldTag<T>),
    New(NewTag<T>),
}

impl<T: Deref<Target = str> + Clone> TryFrom<Tag<T>> for LanguageTag<String> {
    type Error = LanguageTagParseError;

    fn try_from(value: Tag<T>) -> Result<Self, Self::Error> {
        match value {
            Tag::Old(tag) => tag.try_into(),
            Tag::New(tag) => tag.try_into(),
        }
    }
}

pub struct OldTag<T: Deref<Target = str> + Clone>(pub T);
pub struct NewTag<T: Deref<Target = str> + Clone>(pub T);

/// Old-style tags are mainly correct BCP47, apart from two.
/// We also fix als -> gsw here.
impl<T> TryFrom<OldTag<T>> for LanguageTag<String>
where
    T: Deref<Target = str> + Clone,
{
    type Error = LanguageTagParseError;
    fn try_from(tag: OldTag<T>) -> Result<Self, Self::Error> {
        let standard = match &tag.0.deref()[9..] {
            //why deref works here?
            "sh" => "sr-Latn",
            "tl" => "fil",
            "als" => "gsw",
            other => &other,
        };

        Ok(LanguageTag::parse(standard.to_string())?)
    }
}

/// New style tags are correct BCP47 but are in ISO 639-2 which currently
/// makes the comparison hard with old style tags.
/// We convert codes to ISO-639-1 when possible.
impl<T> TryFrom<NewTag<T>> for LanguageTag<String>
where
    T: Deref<Target = str> + Clone,
{
    type Error = LanguageTagParseError;
    fn try_from(tag: NewTag<T>) -> Result<Self, Self::Error> {
        let t: &str = &tag.0[9..]; // coerce into &str
        let standard = NEW_TAG_REPLACE.get(t).unwrap_or(&t);
        Ok(LanguageTag::parse(standard.to_string())?)
    }
}

#[cfg(test)]
mod tests {
    use oxilangtag::LanguageTag;

    use super::{NewTag, OldTag};

    #[test]
    fn test_en() {
        let old_style = LanguageTag::try_from(OldTag("en")).unwrap();
        let new_style = LanguageTag::try_from(NewTag("eng")).unwrap();

        assert_eq!(old_style, new_style);
    }
}
