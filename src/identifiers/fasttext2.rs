// //! Fasttext identifier
// use std::{collections::HashMap, path::Path, str::Lines};

// use crate::{error::Error, lang::Lang};
// use fasttext::{FastText as FastTextLib, Prediction};

use std::collections::HashSet;

// use super::{identifier, Identification, Identifier};
use lazy_static::lazy_static;
use oxilangtag::LanguageTag;

use crate::identifiers::tag_convert::{NewTag, OldTag};
lazy_static! {
    pub static ref OLD_LANGS: HashSet<LanguageTag<String>> = [
        "en", "ru", "de", "fr", "it", "ja", "es", "ceb", "tr", "pt", "uk", "eo", "pl", "sv", "nl",
        "he", "zh", "hu", "ar", "ca", "fi", "cs", "fa", "sr", "el", "vi", "bg", "ko", "no", "mk",
        "ro", "id", "th", "hy", "da", "ta", "hi", "hr", "sr-Latn", "be", "ka", "te", "kk", "war",
        "lt", "gl", "sk", "bn", "eu", "sl", "kn", "ml", "mr", "et", "az", "ms", "sq", "la", "bs",
        "nn", "ur", "lv", "my", "tt", "af", "oc", "nds", "ky", "ast", "fil", "is", "ia", "si",
        "gu", "km", "br", "ba", "uz", "bo", "pa", "vo", "als", "ne", "cy", "jbo", "fy", "mn", "lb",
        "ce", "ug", "tg", "sco", "sa", "cv", "jv", "min", "io", "or", "as", "new", "ga", "mg",
        "an", "ckb", "sw", "bar", "lmo", "yi", "arz", "mhr", "azb", "sah", "pnb", "su", "bpy",
        "pms", "ilo", "wuu", "ku", "ps", "ie", "xmf", "yue", "gom", "li", "mwl", "kw", "sd", "hsb",
        "scn", "gd", "pam", "bh", "mai", "vec", "mt", "dv", "wa", "mzn", "am", "qu", "eml", "cbk",
        "tk", "rm", "os", "vls", "yo", "lo", "lez", "so", "myv", "diq", "mrj", "dsb", "frr", "ht",
        "gn", "bxr", "kv", "sc", "nah", "krc", "bcl", "nap", "gv", "av", "rue", "xal", "pfl",
        "dty", "hif", "co", "lrc", "vep", "tyv"
    ]
    .into_iter()
    .map(|lang| OldTag(String::from("_________") + lang).try_into().unwrap())
    .collect();
    pub static ref NEW_LANGS: HashSet<LanguageTag<String>> = [
        "abk", "ace_Arab", "ace_Latn", "ady", "afr", "aka", "alt", "amh", "ara_Arab", "ara_Latn",
        "arn", "asm", "ast", "awa", "ayr", "azb", "azj", "bak", "bam", "ban", "bel", "bem", "ben",
        "bho", "bis", "bjn_Arab", "bjn_Latn", "bod", "bos", "bug", "bul", "bxr", "cat", "ceb",
        "ces", "che", "chv", "cjk", "ckb", "crh_Latn", "cym", "dan", "deu", "dik", "diq", "dyu",
        "dzo", "ell", "eng", "epo", "est", "eus", "ewe", "ewo", "fao", "fas", "fij", "fin", "fon",
        "fra", "fur", "fuv", "gla", "gle", "glg", "gom", "grn", "guj", "hat", "hau", "heb", "hin",
        "hne", "hrv", "hun", "hye", "ibo", "ilo", "ind", "isl", "ita", "jav", "jpn", "kab", "kac",
        "kal", "kam", "kan", "kas_Arab", "kas_Deva", "kat", "kau_Arab", "kau_Latn", "kaz", "kbp",
        "kea", "khm", "kik", "kin", "kir", "kmb", "kon", "kor", "krc", "kur", "lao", "lav", "lij",
        "lim", "lin", "lit", "lmo", "ltg", "ltz", "lua", "lug", "luo", "lus", "mag", "mai", "mal",
        "mar", "min_Latn", "mkd", "mlg", "mlt", "mni_Mtei", "mon", "mos", "mri", "msa", "mya",
        "nav", "nia", "nld", "nno", "nob", "npi", "nso", "nus", "nya", "oci", "orm", "ory", "oss",
        "pag", "pan", "pap", "pcm", "pol", "por", "prs", "pus", "que", "roh", "ron", "run", "rus",
        "sag", "san", "sat", "scn", "shn", "sin", "slk", "slv", "smo", "sna", "snd", "som", "sot",
        "spa", "sqi", "srd", "srp_Cyrl", "ssw", "sun", "swe", "swh", "szl", "tah", "tam",
        "tat_Cyrl", "tel", "tgk", "tgl", "tha", "tir", "tmh_Latn", "tmh_Tfng", "ton", "tpi", "tsn",
        "tso", "tuk", "tum", "tur", "twi", "tzm", "udm", "uig", "ukr", "umb", "urd", "uzb", "vec",
        "vie", "war", "wes", "wol", "xho", "xmf", "yid", "yor", "yue", "zho_Hans", "zho_Hant",
        "zul",
    ]
    .into_iter()
    .map(|lang| NewTag(String::from("_________") + lang).try_into().unwrap())
    .collect();
}
