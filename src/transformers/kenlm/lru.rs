use std::collections::{HashMap, VecDeque};

use log::{debug, error};

use super::{adult_content::AdultDetectorBuilder, AdultDetector};

struct Models {
    models: HashMap<String, AdultDetector>,
    builders: HashMap<String, AdultDetectorBuilder>,
    //cache: Vec<String>,
    //cache_len: usize,
}

impl Models {
    /// Get model of provided language. Attempts to build it if it is not yet instantiated
    pub fn get(&mut self, lang: &str) -> Option<&AdultDetector> {
        // two step to avoid immutable+mutable borrow
        if !self.models.contains_key(lang) {
            debug!("building KenLM {lang}");
            self.load(lang);
        }
        self.models.get(lang)
    }

    fn unload(&mut self, lang: &str) {
        self.models.remove(lang);
    }

    fn load(&mut self, lang: &str) {
        if let Some(builder) = self.builders.get(lang) {
            self.models.insert(lang.to_owned(), builder.build());
        }
    }
}
