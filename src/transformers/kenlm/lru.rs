use std::collections::HashMap;

use super::{adult_content::AdultDetectorBuilder, AdultDetector};

enum ModelState {
    Loaded((AdultDetector, AdultDetectorBuilder)),
    NotLoaded(AdultDetectorBuilder),
}
struct Models {
    model_list: HashMap<String, Option<AdultDetector>>,
    cache: HashMap<String, usize>, // Lang -> Age
    current_age: usize,
    max_models: usize,
}

impl Models {
    /// oldest == min age
    fn remove_oldest(&mut self) {
        if let Some((oldest, _)) = self
            .cache
            .iter()
            .min_by(|(a, age_a), (b, age_b)| age_a.cmp(age_b))
        {
            self.cache.remove(&*oldest);

            // unload model
            if let Some(detector) = self.model_list.get_mut(&*oldest) {
                *detector = None;
            }
        }
    }
    pub fn get(&mut self, lang: &str) -> &Option<AdultDetector> {
        let entry = self.model_list.get(lang);

        if let Some(entry) = entry {
            //bump model to most recently used
            if let Some(cache_entry) = self.cache.get_mut(lang) {
                *cache_entry = self.current_age;
                self.current_age += 1;
            }
        }

        todo!()
        // match self.model_list.get_mut(lang) {
        //     Some(model) => {
        //         // bump model to most recently used
        //         if let Some(cache_entry) = self.cache.get_mut(lang) {
        //             *cache_entry = self.current_age;
        //             self.current_age += 1;
        //         }
        //         model
        //     }
        //     e => {
        //         // we know it's none
        //         if self.cache.len() >= self.max_models {
        //             self.remove_oldest(); // make place
        //         }

        //         // load model
        //         // TODO find a way to
        //         e = Some(AdultDetector::default());

        //         self.cache.insert(lang.to_string(), self.current_age);
        //         self.current_age += 1;

        //         e
        //     }
        // }
    }
}
