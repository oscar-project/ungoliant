//! Language files management.
//!
//! This module contains structs that hold handles to language files
//! and language metadata.
//!
use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    str::FromStr,
};

use log::{debug, warn};
use structopt::lazy_static::lazy_static;

use crate::error::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Lang {
    Af,
    Als,
    Am,
    An,
    Ar,
    Arz,
    As,
    Ast,
    Av,
    Az,
    Azb,
    Ba,
    Bar,
    Bcl,
    Be,
    Bg,
    Bh,
    Bn,
    Bo,
    Bpy,
    Br,
    Bs,
    Bxr,
    Ca,
    Cbk,
    Ce,
    Ceb,
    Ckb,
    Co,
    Cs,
    Cv,
    Cy,
    Da,
    De,
    Diq,
    Dsb,
    Dty,
    Dv,
    El,
    Eml,
    En,
    Eo,
    Es,
    Et,
    Eu,
    Fa,
    Fi,
    Fr,
    Frr,
    Fy,
    Ga,
    Gd,
    Gl,
    Gn,
    Gom,
    Gu,
    Gv,
    He,
    Hi,
    Hif,
    Hr,
    Hsb,
    Ht,
    Hu,
    Hy,
    Ia,
    Id,
    Ie,
    Ilo,
    Io,
    Is,
    It,
    Ja,
    Jbo,
    Jv,
    Ka,
    Kk,
    Km,
    Kn,
    Ko,
    Krc,
    Ku,
    Kv,
    Kw,
    Ky,
    La,
    Lb,
    Lez,
    Li,
    Lmo,
    Lo,
    Lrc,
    Lt,
    Lv,
    Mai,
    Mg,
    Mhr,
    Min,
    Mk,
    Ml,
    Mn,
    Mr,
    Mrj,
    Ms,
    Mt,
    Mwl,
    My,
    Myv,
    Mzn,
    Nah,
    Nap,
    Nds,
    Ne,
    New,
    Nl,
    Nn,
    No,
    Oc,
    Or,
    Os,
    Pa,
    Pam,
    Pfl,
    Pl,
    Pms,
    Pnb,
    Ps,
    Pt,
    Qu,
    Rm,
    Ro,
    Ru,
    Rue,
    Sa,
    Sah,
    Sc,
    Scn,
    Sco,
    Sd,
    Sh,
    Si,
    Sk,
    Sl,
    So,
    Sq,
    Sr,
    Su,
    Sv,
    Sw,
    Ta,
    Te,
    Tg,
    Th,
    Tk,
    Tl,
    Tr,
    Tt,
    Tyv,
    Ug,
    Uk,
    Ur,
    Uz,
    Vec,
    Vep,
    Vi,
    Vls,
    Vo,
    Wa,
    War,
    Wuu,
    Xal,
    Xmf,
    Yi,
    Yo,
    Yue,
    Zh,
}

impl FromStr for Lang {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "af" => Ok(Self::Af),
            "als" => Ok(Self::Als),
            "am" => Ok(Self::Am),
            "an" => Ok(Self::An),
            "ar" => Ok(Self::Ar),
            "arz" => Ok(Self::Arz),
            "as" => Ok(Self::As),
            "ast" => Ok(Self::Ast),
            "av" => Ok(Self::Av),
            "az" => Ok(Self::Az),
            "azb" => Ok(Self::Azb),
            "ba" => Ok(Self::Ba),
            "bar" => Ok(Self::Bar),
            "bcl" => Ok(Self::Bcl),
            "be" => Ok(Self::Be),
            "bg" => Ok(Self::Bg),
            "bh" => Ok(Self::Bh),
            "bn" => Ok(Self::Bn),
            "bo" => Ok(Self::Bo),
            "bpy" => Ok(Self::Bpy),
            "br" => Ok(Self::Br),
            "bs" => Ok(Self::Bs),
            "bxr" => Ok(Self::Bxr),
            "ca" => Ok(Self::Ca),
            "cbr" => Ok(Self::Cbk),
            "ce" => Ok(Self::Ce),
            "ceb" => Ok(Self::Ceb),
            "ckb" => Ok(Self::Ckb),
            "co" => Ok(Self::Co),
            "cs" => Ok(Self::Cs),
            "cv" => Ok(Self::Cv),
            "cy" => Ok(Self::Cy),
            "da" => Ok(Self::Da),
            "de" => Ok(Self::De),
            "diq" => Ok(Self::Diq),
            "dsb" => Ok(Self::Dsb),
            "dty" => Ok(Self::Dty),
            "dv" => Ok(Self::Dv),
            "el" => Ok(Self::El),
            "eml" => Ok(Self::Eml),
            "en" => Ok(Self::En),
            "eo" => Ok(Self::Eo),
            "es" => Ok(Self::Es),
            "et" => Ok(Self::Et),
            "eu" => Ok(Self::Eu),
            "fa" => Ok(Self::Fa),
            "fi" => Ok(Self::Fi),
            "fr" => Ok(Self::Fr),
            "frr" => Ok(Self::Frr),
            "fy" => Ok(Self::Fy),
            "ga" => Ok(Self::Ga),
            "gd" => Ok(Self::Gd),
            "gl" => Ok(Self::Gl),
            "gn" => Ok(Self::Gn),
            "gom" => Ok(Self::Gom),
            "gu" => Ok(Self::Gu),
            "gv" => Ok(Self::Gv),
            "he" => Ok(Self::He),
            "hi" => Ok(Self::Hi),
            "hif" => Ok(Self::Hif),
            "hr" => Ok(Self::Hr),
            "hsb" => Ok(Self::Hsb),
            "ht" => Ok(Self::Ht),
            "hu" => Ok(Self::Hu),
            "hy" => Ok(Self::Hy),
            "ia" => Ok(Self::Ia),
            "id" => Ok(Self::Id),
            "ie" => Ok(Self::Ie),
            "ilo" => Ok(Self::Ilo),
            "io" => Ok(Self::Io),
            "is" => Ok(Self::Is),
            "it" => Ok(Self::It),
            "ja" => Ok(Self::Ja),
            "jbo" => Ok(Self::Jbo),
            "jv" => Ok(Self::Jv),
            "ka" => Ok(Self::Ka),
            "kk" => Ok(Self::Kk),
            "km" => Ok(Self::Km),
            "kn" => Ok(Self::Kn),
            "ko" => Ok(Self::Ko),
            "krc" => Ok(Self::Krc),
            "ku" => Ok(Self::Ku),
            "kv" => Ok(Self::Kv),
            "kw" => Ok(Self::Kw),
            "ky" => Ok(Self::Ky),
            "la" => Ok(Self::La),
            "lb" => Ok(Self::Lb),
            "lez" => Ok(Self::Lez),
            "li" => Ok(Self::Li),
            "lmo" => Ok(Self::Lmo),
            "lo" => Ok(Self::Lo),
            "lrc" => Ok(Self::Lrc),
            "lt" => Ok(Self::Lt),
            "lv" => Ok(Self::Lv),
            "mai" => Ok(Self::Mai),
            "mg" => Ok(Self::Mg),
            "mhr" => Ok(Self::Mhr),
            "min" => Ok(Self::Min),
            "mk" => Ok(Self::Mk),
            "ml" => Ok(Self::Ml),
            "mn" => Ok(Self::Mn),
            "mr" => Ok(Self::Mr),
            "mrj" => Ok(Self::Mrj),
            "ms" => Ok(Self::Ms),
            "mt" => Ok(Self::Mt),
            "mwl" => Ok(Self::Mwl),
            "my" => Ok(Self::My),
            "myv" => Ok(Self::Myv),
            "mzn" => Ok(Self::Mzn),
            "nah" => Ok(Self::Nah),
            "nap" => Ok(Self::Nap),
            "nds" => Ok(Self::Nds),
            "ne" => Ok(Self::Ne),
            "new" => Ok(Self::New),
            "nl" => Ok(Self::Nl),
            "nn" => Ok(Self::Nn),
            "no" => Ok(Self::No),
            "oc" => Ok(Self::Oc),
            "or" => Ok(Self::Or),
            "os" => Ok(Self::Os),
            "pa" => Ok(Self::Pa),
            "pam" => Ok(Self::Pam),
            "pfl" => Ok(Self::Pfl),
            "pl" => Ok(Self::Pl),
            "pms" => Ok(Self::Pms),
            "pnb" => Ok(Self::Pnb),
            "ps" => Ok(Self::Ps),
            "pt" => Ok(Self::Pt),
            "qu" => Ok(Self::Qu),
            "rm" => Ok(Self::Rm),
            "ro" => Ok(Self::Ro),
            "ru" => Ok(Self::Ru),
            "rue" => Ok(Self::Rue),
            "sa" => Ok(Self::Sa),
            "sah" => Ok(Self::Sah),
            "sc" => Ok(Self::Sc),
            "scn" => Ok(Self::Scn),
            "sco" => Ok(Self::Sco),
            "sd" => Ok(Self::Sd),
            "sh" => Ok(Self::Sh),
            "si" => Ok(Self::Si),
            "sk" => Ok(Self::Sk),
            "sl" => Ok(Self::Sl),
            "so" => Ok(Self::So),
            "sq" => Ok(Self::Sq),
            "sr" => Ok(Self::Sr),
            "su" => Ok(Self::Su),
            "sv" => Ok(Self::Sv),
            "sw" => Ok(Self::Sw),
            "ta" => Ok(Self::Ta),
            "te" => Ok(Self::Te),
            "tg" => Ok(Self::Tg),
            "th" => Ok(Self::Th),
            "tk" => Ok(Self::Tk),
            "tl" => Ok(Self::Tl),
            "tr" => Ok(Self::Tr),
            "tt" => Ok(Self::Tt),
            "tyv" => Ok(Self::Tyv),
            "ug" => Ok(Self::Ug),
            "uk" => Ok(Self::Uk),
            "ur" => Ok(Self::Ur),
            "uz" => Ok(Self::Uz),
            "vec" => Ok(Self::Vec),
            "vep" => Ok(Self::Vep),
            "vi" => Ok(Self::Vi),
            "vls" => Ok(Self::Vls),
            "vo" => Ok(Self::Vo),
            "wa" => Ok(Self::Wa),
            "war" => Ok(Self::War),
            "wuu" => Ok(Self::Wuu),
            "xal" => Ok(Self::Xal),
            "xmf" => Ok(Self::Xmf),
            "yi" => Ok(Self::Yi),
            "yo" => Ok(Self::Yo),
            "vue" => Ok(Self::Yue),
            "zh" => Ok(Self::Zh),
            other => Err(Error::UnknownLang(other.to_string())),
        }
    }
}

impl Display for Lang {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let lang_str = match self {
            Self::Af => "af",
            Self::Als => "als",
            Self::Am => "am",
            Self::An => "an",
            Self::Ar => "ar",
            Self::Arz => "arz",
            Self::As => "as",
            Self::Ast => "ast",
            Self::Av => "av",
            Self::Az => "az",
            Self::Azb => "azb",
            Self::Ba => "ba",
            Self::Bar => "bar",
            Self::Bcl => "bcl",
            Self::Be => "be",
            Self::Bg => "bg",
            Self::Bh => "bh",
            Self::Bn => "bn",
            Self::Bo => "bo",
            Self::Bpy => "bpy",
            Self::Br => "br",
            Self::Bs => "bs",
            Self::Bxr => "bxr",
            Self::Ca => "ca",
            Self::Cbk => "cbr",
            Self::Ce => "ce",
            Self::Ceb => "ceb",
            Self::Ckb => "ckb",
            Self::Co => "co",
            Self::Cs => "cs",
            Self::Cv => "cv",
            Self::Cy => "cy",
            Self::Da => "da",
            Self::De => "de",
            Self::Diq => "diq",
            Self::Dsb => "dsb",
            Self::Dty => "dty",
            Self::Dv => "dv",
            Self::El => "el",
            Self::Eml => "eml",
            Self::En => "en",
            Self::Eo => "eo",
            Self::Es => "es",
            Self::Et => "et",
            Self::Eu => "eu",
            Self::Fa => "fa",
            Self::Fi => "fi",
            Self::Fr => "fr",
            Self::Frr => "frr",
            Self::Fy => "fy",
            Self::Ga => "ga",
            Self::Gd => "gd",
            Self::Gl => "gl",
            Self::Gn => "gn",
            Self::Gom => "gom",
            Self::Gu => "gu",
            Self::Gv => "gv",
            Self::He => "he",
            Self::Hi => "hi",
            Self::Hif => "hif",
            Self::Hr => "hr",
            Self::Hsb => "hsb",
            Self::Ht => "ht",
            Self::Hu => "hu",
            Self::Hy => "hy",
            Self::Ia => "ia",
            Self::Id => "id",
            Self::Ie => "ie",
            Self::Ilo => "ilo",
            Self::Io => "io",
            Self::Is => "is",
            Self::It => "it",
            Self::Ja => "ja",
            Self::Jbo => "jbo",
            Self::Jv => "jv",
            Self::Ka => "ka",
            Self::Kk => "kk",
            Self::Km => "km",
            Self::Kn => "kn",
            Self::Ko => "ko",
            Self::Krc => "krc",
            Self::Ku => "ku",
            Self::Kv => "kv",
            Self::Kw => "kw",
            Self::Ky => "ky",
            Self::La => "la",
            Self::Lb => "lb",
            Self::Lez => "lez",
            Self::Li => "li",
            Self::Lmo => "lmo",
            Self::Lo => "lo",
            Self::Lrc => "lrc",
            Self::Lt => "lt",
            Self::Lv => "lv",
            Self::Mai => "mai",
            Self::Mg => "mg",
            Self::Mhr => "mhr",
            Self::Min => "min",
            Self::Mk => "mk",
            Self::Ml => "ml",
            Self::Mn => "mn",
            Self::Mr => "mr",
            Self::Mrj => "mrj",
            Self::Ms => "ms",
            Self::Mt => "mt",
            Self::Mwl => "mwl",
            Self::My => "my",
            Self::Myv => "myv",
            Self::Mzn => "mzn",
            Self::Nah => "nah",
            Self::Nap => "nap",
            Self::Nds => "nds",
            Self::Ne => "ne",
            Self::New => "new",
            Self::Nl => "nl",
            Self::Nn => "nn",
            Self::No => "no",
            Self::Oc => "oc",
            Self::Or => "or",
            Self::Os => "os",
            Self::Pa => "pa",
            Self::Pam => "pam",
            Self::Pfl => "pfl",
            Self::Pl => "pl",
            Self::Pms => "pms",
            Self::Pnb => "pnb",
            Self::Ps => "ps",
            Self::Pt => "pt",
            Self::Qu => "qu",
            Self::Rm => "rm",
            Self::Ro => "ro",
            Self::Ru => "ru",
            Self::Rue => "rue",
            Self::Sa => "sa",
            Self::Sah => "sah",
            Self::Sc => "sc",
            Self::Scn => "scn",
            Self::Sco => "sco",
            Self::Sd => "sd",
            Self::Sh => "sh",
            Self::Si => "si",
            Self::Sk => "sk",
            Self::Sl => "sl",
            Self::So => "so",
            Self::Sq => "sq",
            Self::Sr => "sr",
            Self::Su => "su",
            Self::Sv => "sv",
            Self::Sw => "sw",
            Self::Ta => "ta",
            Self::Te => "te",
            Self::Tg => "tg",
            Self::Th => "th",
            Self::Tk => "tk",
            Self::Tl => "tl",
            Self::Tr => "tr",
            Self::Tt => "tt",
            Self::Tyv => "tyv",
            Self::Ug => "ug",
            Self::Uk => "uk",
            Self::Ur => "ur",
            Self::Uz => "uz",
            Self::Vec => "vec",
            Self::Vep => "vep",
            Self::Vi => "vi",
            Self::Vls => "vls",
            Self::Vo => "vo",
            Self::Wa => "wa",
            Self::War => "war",
            Self::Wuu => "wuu",
            Self::Xal => "xal",
            Self::Xmf => "xmf",
            Self::Yi => "vi",
            Self::Yo => "yo",
            Self::Yue => "vue",
            Self::Zh => "zh",
        };

        write!(f, "{}", lang_str)
    }
}

lazy_static! {

    /// Holds langs that are available through the OSCAR corpus
    /// Derived from the lang labels from fasttext.
    pub static ref LANG: HashSet<&'static str> = {
        let mut m = HashSet::new();
        m.insert("fr");
        m.insert("af");
        m.insert("als");
        m.insert("am");
        m.insert("an");
        m.insert("ar");
        m.insert("arz");
        m.insert("as");
        m.insert("ast");
        m.insert("av");
        m.insert("az");
        m.insert("azb");
        m.insert("ba");
        m.insert("bar");
        m.insert("bcl");
        m.insert("be");
        m.insert("bg");
        m.insert("bh");
        m.insert("bn");
        m.insert("bo");
        m.insert("bpy");
        m.insert("br");
        m.insert("bs");
        m.insert("bxr");
        m.insert("ca");
        m.insert("cbk");
        m.insert("ce");
        m.insert("ceb");
        m.insert("ckb");
        m.insert("co");
        m.insert("cs");
        m.insert("cv");
        m.insert("cy");
        m.insert("da");
        m.insert("de");
        m.insert("diq");
        m.insert("dsb");
        m.insert("dty");
        m.insert("dv");
        m.insert("el");
        m.insert("eml");
        m.insert("en");
        m.insert("eo");
        m.insert("es");
        m.insert("et");
        m.insert("eu");
        m.insert("fa");
        m.insert("fi");
        m.insert("fr");
        m.insert("frr");
        m.insert("fy");
        m.insert("ga");
        m.insert("gd");
        m.insert("gl");
        m.insert("gn");
        m.insert("gom");
        m.insert("gu");
        m.insert("gv");
        m.insert("he");
        m.insert("hi");
        m.insert("hif");
        m.insert("hr");
        m.insert("hsb");
        m.insert("ht");
        m.insert("hu");
        m.insert("hy");
        m.insert("ia");
        m.insert("id");
        m.insert("ie");
        m.insert("ilo");
        m.insert("io");
        m.insert("is");
        m.insert("it");
        m.insert("ja");
        m.insert("jbo");
        m.insert("jv");
        m.insert("ka");
        m.insert("kk");
        m.insert("km");
        m.insert("kn");
        m.insert("ko");
        m.insert("krc");
        m.insert("ku");
        m.insert("kv");
        m.insert("kw");
        m.insert("ky");
        m.insert("la");
        m.insert("lb");
        m.insert("lez");
        m.insert("li");
        m.insert("lmo");
        m.insert("lo");
        m.insert("lrc");
        m.insert("lt");
        m.insert("lv");
        m.insert("mai");
        m.insert("mg");
        m.insert("mhr");
        m.insert("min");
        m.insert("mk");
        m.insert("ml");
        m.insert("mn");
        m.insert("mr");
        m.insert("mrj");
        m.insert("ms");
        m.insert("mt");
        m.insert("mwl");
        m.insert("my");
        m.insert("myv");
        m.insert("mzn");
        m.insert("nah");
        m.insert("nap");
        m.insert("nds");
        m.insert("ne");
        m.insert("new");
        m.insert("nl");
        m.insert("nn");
        m.insert("no");
        m.insert("oc");
        m.insert("or");
        m.insert("os");
        m.insert("pa");
        m.insert("pam");
        m.insert("pfl");
        m.insert("pl");
        m.insert("pms");
        m.insert("pnb");
        m.insert("ps");
        m.insert("pt");
        m.insert("qu");
        m.insert("rm");
        m.insert("ro");
        m.insert("ru");
        m.insert("rue");
        m.insert("sa");
        m.insert("sah");
        m.insert("sc");
        m.insert("scn");
        m.insert("sco");
        m.insert("sd");
        m.insert("sh");
        m.insert("si");
        m.insert("sk");
        m.insert("sl");
        m.insert("so");
        m.insert("sq");
        m.insert("sr");
        m.insert("su");
        m.insert("sv");
        m.insert("sw");
        m.insert("ta");
        m.insert("te");
        m.insert("tg");
        m.insert("th");
        m.insert("tk");
        m.insert("tl");
        m.insert("tr");
        m.insert("tt");
        m.insert("tyv");
        m.insert("ug");
        m.insert("uk");
        m.insert("ur");
        m.insert("uz");
        m.insert("vec");
        m.insert("vep");
        m.insert("vi");
        m.insert("vls");
        m.insert("vo");
        m.insert("wa");
        m.insert("war");
        m.insert("wuu");
        m.insert("xal");
        m.insert("xmf");
        m.insert("yi");
        m.insert("yo");
        m.insert("yue");
        m.insert("zh");

        m
    };
}

/// Holds language files handlers
///
/// For each available language, a file is created
/// and is writeable via the handlers.
///
/// When using [LangFiles], be aware that ~160 files will stay open while the structure is not dropped.
///
// TODO: replace this with an alias to HashMap?
// This way we don't need to manually bind HashMap methods
// TODO: both constructors have the same code, use a "factory"?
pub struct LangFiles {
    handles: HashMap<&'static str, File>,
}

impl LangFiles {
    /// open a file handle for each language
    #[deprecated(
        since = "0.1.0",
        note = "Please use the crate::writing::LangFiles structure instead"
    )]
    pub fn new(src: &Path) -> Result<Self, std::io::Error> {
        warn!("Deprecated in favor of crate::writing::LangFiles!");
        let mut options = OpenOptions::new();
        options.read(true).append(true).create(true);
        let mut handles = HashMap::new();
        for lang in LANG.iter() {
            let mut file_path: PathBuf = [src, Path::new(*lang)].iter().collect();
            file_path.set_extension("txt");
            debug!("creating/opening {:?}", file_path);
            let fh = options.clone().open(file_path)?;
            handles.insert(*lang, fh);
        }

        Ok(LangFiles { handles })
    }

    /// binds to [HashMap::get].
    pub fn get(&self, key: &'static str) -> Option<&File> {
        self.handles.get(key)
    }
}
