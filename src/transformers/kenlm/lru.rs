use std::{
    collections::{hash_map::Entry, HashMap, VecDeque},
    ffi::OsStr,
    path::Path,
    sync::{Arc, RwLock, RwLockReadGuard},
};

use log::{debug, error, warn};
use oxilangtag::LanguageTag;

use crate::pipelines::oscardoc::types::{Document, Location};

use super::{
    adult_content::{self, AdultDetectorBuilder},
    AdultDetector,
};

use lazy_static::lazy_static;

lazy_static! {
    static ref KENLM_EXTS: [Option<&'static OsStr>; 2] =
        [Some(OsStr::new("arpa")), Some(OsStr::new("binary"))];
}

#[derive(Debug)]
pub enum Error {
    NoBuilder(String),
    Building(std::io::Error),
}
impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Building(err)
    }
}
/// Model holder.
/// Internally has two [HashMap]: One with builders and one with the actual models.
///
/// This is to lazily load models as we need them, and it should help build a LRU-like caching system so that RAM usage doesn't explode.
#[derive(Default)]
pub struct Models {
    models: Arc<RwLock<HashMap<LanguageTag<String>, Arc<RwLock<AdultDetector>>>>>,
    builders: Arc<RwLock<HashMap<LanguageTag<String>, Arc<RwLock<AdultDetectorBuilder>>>>>,
}

impl Models {
    /// Get a read lock on loaded models.
    pub fn models(
        &self,
    ) -> RwLockReadGuard<HashMap<LanguageTag<String>, Arc<RwLock<AdultDetector>>>> {
        self.models.read().unwrap()
    }

    /// Create a new `Models` struct already populated by builders for langs/models present in provided directory.
    pub fn from_dir(dir: &Path) -> std::io::Result<Self> {
        let mut builders: HashMap<LanguageTag<_>, AdultDetectorBuilder> = HashMap::new();

        // iterate over entries in kenlms path
        for direntry in std::fs::read_dir(&dir)? {
            if let Ok(direntry) = direntry {
                let model_path = direntry.path();
                if !model_path.is_file() {
                    debug!("{model_path:?} is not a file, skipping");
                    continue;
                }

                // skip files that are not arpa or binary
                if !KENLM_EXTS.contains(&model_path.extension()) {
                    warn!("{model_path:?} is not a KenLM model file, skipping");
                    continue;
                }

                //  get model name
                let model_name = model_path.file_stem();

                if model_name.is_none() {
                    warn!("Couldn't find a model name for {model_path:?}, skipping");
                    continue;
                }

                let model_name = model_name.unwrap().to_string_lossy().to_string();

                // try to parse the model name into a language
                if let Ok(model_name) = LanguageTag::parse(model_name.to_owned()) {
                    match builders.entry(model_name) {
                        // if we already have one, check file extension
                        Entry::Occupied(mut o) => {
                            // if we have a builder on arpa model, replace by binary model.
                            if o.get().path().extension() == KENLM_EXTS[0] {
                                o.get_mut().set_path(&model_path);
                            }
                        }

                        // insert
                        Entry::Vacant(v) => {
                            v.insert(AdultDetectorBuilder::new(model_path.to_path_buf()));
                        }
                    }
                } else {
                    warn!("Couldn't parse {model_name:?} into a proper language tag, skipping");
                    continue;
                }
            }
        }

        debug!(
            "Got {} KenLMs for the following languages: {:?}",
            builders.len(),
            builders.keys().collect::<Vec<_>>()
        );

        // wrap into arc+rwlock.
        // TODO: maybe remove arc+rwlock on the HM and/or on the models, since after this step there's no write access,
        //       or keep it for LRU cache impl later on.
        let builders = builders
            .into_iter()
            .map(|(name, builder)| (name, Arc::new(RwLock::new(builder))))
            .collect();

        Ok(Models {
            builders: Arc::new(RwLock::new(builders)),
            ..Default::default()
        })
    }

    /// Check if there is a builder for a given language.
    pub fn contains(&self, lang: &LanguageTag<String>) -> bool {
        self.builders
            .read()
            .expect("Problem locking builders (in read)")
            .contains_key(lang)
    }

    /// Check if there is a loaded model for a given language.
    /// Note that the behaviour is similar if:
    /// - There is no builder for the given language
    /// - There is no loaded model for the given language.
    ///
    /// Use [Models::contains] to check for the presence of a builder specifically.
    pub fn is_loaded(&self, lang: &str) -> bool {
        self.models
            .read()
            .expect("Problem locking models (in read)")
            .contains_key(lang)
    }

    /// Insert a new builder for a given language.
    /// Behaves like [HashMap::insert].
    ///
    /// Be aware that you'll have to call [Models::get] to actually build the model.
    pub fn insert_builder(&self, lang: &LanguageTag<String>, builder: AdultDetectorBuilder) {
        debug!("Creating builder for {lang}");
        let mut builders_lock = self.builders.write().unwrap();
        builders_lock.insert(lang.to_owned(), Arc::new(RwLock::new(builder)));
    }

    /// Load a model by using this language's builder.
    pub fn load(&self, lang: &LanguageTag<String>) -> Result<(), Error> {
        debug!("Loading model {lang} in memory");
        let builders = self.builders.read().unwrap();
        if let Some(builder) = builders.get(lang) {
            let builder = builder.write().unwrap();
            let mut models = self.models.write().unwrap();
            models.insert(lang.to_owned(), Arc::new(RwLock::new(builder.build()?)));
            Ok(())
        } else {
            debug!("Could not load model for lang {lang}");
            Err(Error::NoBuilder(format!("No builder found for {lang:?}")))
        }
    }

    /// Unload a model.
    fn unload(&self, lang: &str) {
        debug!("Unloading model {lang} from memory");
        let mut models = self.models.write().unwrap();
        models.remove(lang);
    }
}
