//! sentence-level filtering
use super::filter::FilterMut;
use super::Filter;
use std::convert::TryInto;

/// regroups sentence filter kinds
enum FilterKind {
    Length(Length),
    MeanLength(MeanLength),
}

/// Simple length filter.
/// Returns `false` if provided sentence is less than [Length::min_size] unicode codepoints.
///
/// [Length::min_size] is 100 by default.
pub struct Length {
    min_size: usize,
}

impl Length {
    /// specify a minimum length
    pub fn with_min_size(min_size: usize) -> Self {
        Self { min_size }
    }

    /// Get a reference to the length's min size.
    pub fn min_size(&self) -> &usize {
        &self.min_size
    }
}

impl Filter<&str> for Length {
    fn detect(&self, sentence: &str) -> bool {
        sentence.chars().count() > self.min_size
    }
}

impl Default for Length {
    /// Default minimum length for sentences is 100 Unicode Codepoints
    fn default() -> Self {
        Length { min_size: 100 }
    }
}

/// Mean filter: Keeps track of mean length of proposed sentences
///
/// Detects sentences that are within the stdandard deviation.
///
/// Implements both [super::Filter] and [super::FilterMut]
pub struct MeanLength {
    nb_measures: u32,
    sum_lenghts: f64,
    pow_lengths: f64,
    mean: f64,
    std: f64,
}

/// mean/std update formula from
/// https://math.stackexchange.com/a/2148949
impl MeanLength {
    /// Updates the stdandard deviation.
    /// has to be used *after* updating the mean.
    fn update_std(&mut self) {
        let pow_div = self.pow_lengths / f64::from(self.nb_measures);
        let sum_div = self.sum_lenghts / f64::from(self.nb_measures);

        self.std = (pow_div - sum_div.powi(2)).sqrt();
    }

    /// updates mean, then standard deviation.
    fn update_mean(&mut self, val: u32) {
        self.nb_measures += 1;
        self.sum_lenghts += f64::from(val);
        self.pow_lengths += f64::from(val.pow(2));

        self.mean = self.sum_lenghts / f64::from(self.nb_measures);
        self.update_std();
    }

    /// Get a reference to the mean length's mean.
    fn mean(&self) -> &f64 {
        &self.mean
    }

    /// Get a reference to the mean length's std.
    fn std(&self) -> &f64 {
        &self.std
    }
}
impl Default for MeanLength {
    fn default() -> Self {
        MeanLength {
            nb_measures: 0,
            sum_lenghts: 0.0,
            pow_lengths: 0.0,
            mean: 0.0,
            std: 0.0,
        }
    }
}

impl FilterMut<&str> for MeanLength {
    fn detect_mut(&mut self, sentence: &str) -> bool {
        // get length and update mean
        let length = sentence.chars().count().try_into().unwrap_or_default();
        self.update_mean(length);

        // ensure that mu-sig<length<mu+sig (eq.to 0<length-mu<sig)
        (f64::from(length) - self.mean) < self.std
    }
}

impl Filter<&str> for MeanLength {
    fn detect(&self, sentence: &str) -> bool {
        let length: u32 = sentence.chars().count().try_into().unwrap_or_default();
        (f64::from(length) - self.mean) < self.std
    }
}

#[cfg(test)]
mod tests {
    use rand::thread_rng;
    use rand_distr::{Distribution, Normal};

    use super::{Filter, Length, MeanLength};
    use crate::filtering::filter::FilterMut;

    #[test]
    fn length_default() {
        let valid: String = ['z'; 101].iter().collect();
        let invalid: String = ['z'; 99].iter().collect();

        let f = Length::default();
        assert_eq!(true, f.detect(&valid));
        assert_eq!(false, f.detect(&invalid));
    }

    #[test]
    fn mean_default() {
        let mut rng = thread_rng();
        let normal = Normal::new(100.0, 10.0).unwrap();

        // sample normal distribution for sentence lengths
        let samples: Vec<f32> = normal.sample_iter(&mut rng).take(100_000).collect();

        // build sentences
        let sentences: Vec<String> = samples
            .iter()
            .map(|sample: &f32| {
                let length = sample.floor() as usize;
                ['a'].iter().cycle().take(length).collect()
            })
            .collect();

        // init filter and feed sentences
        let mut f = MeanLength::default();
        for sentence in sentences {
            f.detect_mut(&sentence);
        }

        // create two obvious examples that are resp. valid and invalid
        let valid: String = ['a'].iter().cycle().take(105).collect();
        let invalid: String = ['a'].iter().cycle().take(130).collect();

        // in case of failure, this will be printed
        println!("init rng   : mu:{:.3} sig:{:.3}", 100.0, 10.0);
        println!("from filter: mu:{:.3} sig:{:.3}", f.mean(), f.std());

        // ensure distribution is correctly learnt
        assert_eq!(f.detect(&valid), true);
        assert_eq!(f.detect(&invalid), false);
    }
}
