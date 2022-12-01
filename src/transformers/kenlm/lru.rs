use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, RwLock, RwLockReadGuard},
};

use log::{debug, error};
use oxilangtag::LanguageTag;

use crate::pipelines::oscardoc::types::{Document, Location};

use super::{
    adult_content::{self, AdultDetectorBuilder},
    AdultDetector,
};

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

    /// Insert the default builder for a given language.
    /// See [AdultDetectorBuilder::default] to check the default values.
    /// Behaves like [HashMap::insert].
    ///
    /// Be aware that you'll have to call [Models::get] to actually build the model.
    pub fn insert_default_builder(&self, lang: &LanguageTag<String>) {
        debug!("Creating default builder for {lang}");
        let mut builders_lock = self.builders.write().unwrap();
        builders_lock.insert(
            lang.to_owned(),
            Arc::new(RwLock::new(AdultDetectorBuilder::default())),
        );
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
            error!("Could not load model for lang {lang}");
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
