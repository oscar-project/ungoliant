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
        "dty", "hif", "co", "lrc", "vep", "tyv", "multi"
    ]
    .into_iter()
    .map(|lang| OldTag(String::from("_________") + lang).try_into().unwrap())
    .collect();


    pub static ref NEW_LANGS: HashSet<LanguageTag<String>> = [
        "__label__eng", "__label__ita", "__label__deu", "__label__fra", "__label__spa", "__label__swe",
        "__label__por", "__label__rus", "__label__pol", "__label__nld", "__label__ukr",
        "__label__srp", "__label__ara", "__label__fin", "__label__hun", "__label__nor",
        "__label__ell", "__label__vie", "__label__dan", "__label__ces", "__label__kor",
        "__label__fas", "__label__ron", "__label__heb", "__label__cat", "__label__tur",
        "__label__ind", "__label__bul", "__label__slv", "__label__hrv", "__label__ceb",
        "__label__slk", "__label__tam", "__label__tha", "__label__hye", "__label__tgl",
        "__label__afr", "__label__est", "__label__hin", "__label__lit", "__label__war",
        "__label__zul", "__label__ilo", "__label__kat", "__label__jpn", "__label__epo",
        "__label__mkd", "__label__swh", "__label__mya", "__label__sot", "__label__tsn",
        "__label__xho", "__label__kaz", "__label__sqi", "__label__lav", "__label__tso",
        "__label__sna", "__label__mal", "__label__amh", "__label__sin", "__label__ben",
        "__label__msa", "__label__tel", "__label__ewe", "__label__tah", "__label__urd",
        "__label__nso", "__label__bis", "__label__kan", "__label__lin", "__label__isl",
        "__label__twi", "__label__mlg", "__label__azj", "__label__pan", "__label__bel",
        "__label__mar", "__label__tpi", "__label__yor", "__label__npi", "__label__eus",
        "__label__bem", "__label__pap", "__label__kin", "__label__guj", "__label__smo",
        "__label__mlt", "__label__che", "__label__run", "__label__ast", "__label__tat",
        "__label__fij", "__label__tir", "__label__ibo", "__label__glg", "__label__kir",
        "__label__lua", "__label__pag", "__label__sag", "__label__oss", "__label__tgk",
        "__label__azb", "__label__mon", "__label__tum", "__label__lug", "__label__umb",
        "__label__nno", "__label__hat", "__label__kal", "__label__kon", "__label__mos",
        "__label__hau", "__label__bak", "__label__lus", "__label__oci", "__label__bos",
        "__label__grn", "__label__orm", "__label__chv", "__label__cym", "__label__khm",
        "__label__aym", "__label__tuk", "__label__luo", "__label__zho", "__label__que",
        "__label__ssw", "__label__uzb", "__label__kik", "__label__jav", "__label__kmb",
        "__label__asm", "__label__ltz", "__label__tog", "__label__yue", "__label__nya",
        "__label__kam", "__label__ckb", "__label__san", "__label__lmo", "__label__sun",
        "__label__min", "__label__gle", "__label__arz", "__label__bod", "__label__xmf",
        "__label__ory", "__label__cjk", "__label__nia", "__label__mai", "__label__lao",
        "__label__fon", "__label__kbp", "__label__pus", "__label__yid", "__label__kur",
        "__label__abk", "__label__uig", "__label__scn", "__label__lim", "__label__snd",
        "__label__wes", "__label__arn", "__label__pcm", "__label__vec", "__label__nav",
        "__label__gom", "__label__dyu", "__label__bho", "__label__gla", "__label__kac",
        "__label__roh", "__label__udm", "__label__kab", "__label__zza", "__label__som",
        "__label__nah", "__label__bxr", "__label__kea", "__label__srd", "__label__krc",
        "__label__alt", "__label__sat", "__label__wol", "__label__fao", "__label__ful",
        "__label__mri", "__label__ewo", "__label__ady", "__label__multi"
    ]
    .into_iter()
    .map(|lang| NewTag(lang).try_into().unwrap())
    .collect();
    // pub static ref NEW_LANGS: HashSet<LanguageTag<String>> = [
    //     "abk", "ace_Arab", "ace_Latn", "ady", "afr", "aka", "alt", "amh", "ara_Arab", "ara_Latn",
    //     "arn", "asm", "ast", "awa", "ayr", "azb", "azj", "bak", "bam", "ban", "bel", "bem", "ben",
    //     "bho", "bis", "bjn_Arab", "bjn_Latn", "bod", "bos", "bug", "bul", "bxr", "cat", "ceb",
    //     "ces", "che", "chv", "cjk", "ckb", "crh_Latn", "cym", "dan", "deu", "dik", "diq", "dyu",
    //     "dzo", "ell", "eng", "epo", "est", "eus", "ewe", "ewo", "fao", "fas", "fij", "fin", "fon",
    //     "fra", "fur", "fuv", "gla", "gle", "glg", "gom", "grn", "guj", "hat", "hau", "heb", "hin",
    //     "hne", "hrv", "hun", "hye", "ibo", "ilo", "ind", "isl", "ita", "jav", "jpn", "kab", "kac",
    //     "kal", "kam", "kan", "kas_Arab", "kas_Deva", "kat", "kau_Arab", "kau_Latn", "kaz", "kbp",
    //     "kea", "khm", "kik", "kin", "kir", "kmb", "kon", "kor", "krc", "kur", "lao", "lav", "lij",
    //     "lim", "lin", "lit", "lmo", "ltg", "ltz", "lua", "lug", "luo", "lus", "mag", "mai", "mal",
    //     "mar", "min_Latn", "mkd", "mlg", "mlt", "mni_Mtei", "mon", "mos", "mri", "msa", "mya",
    //     "nav", "nia", "nld", "nno", "nob", "npi", "nso", "nus", "nya", "oci", "orm", "ory", "oss",
    //     "pag", "pan", "pap", "pcm", "pol", "por", "prs", "pus", "que", "roh", "ron", "run", "rus",
    //     "sag", "san", "sat", "scn", "shn", "sin", "slk", "slv", "smo", "sna", "snd", "som", "sot",
    //     "spa", "sqi", "srd", "srp_Cyrl", "ssw", "sun", "swe", "swh", "szl", "tah", "tam",
    //     "tat_Cyrl", "tel", "tgk", "tgl", "tha", "tir", "tmh_Latn", "tmh_Tfng", "ton", "tpi", "tsn",
    //     "tso", "tuk", "tum", "tur", "twi", "tzm", "udm", "uig", "ukr", "umb", "urd", "uzb", "vec",
    //     "vie", "war", "wes", "wol", "xho", "xmf", "yid", "yor", "yue", "zho_Hans", "zho_Hant",
    //     "zul", "multi"
    // ]
    // .into_iter()
    // .map(|lang| NewTag(String::from("_________") + lang).try_into().unwrap())
    // .collect();
}
