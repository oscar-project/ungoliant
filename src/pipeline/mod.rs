//! Pipelines.
//!
//! Various pipelines are implemented here, and the module
//! provides a light [pipeline::Pipeline] trait that enables easy and flexible pipeline creation.
mod doc;
pub mod oscar_metadata;
#[allow(clippy::module_inception)]
pub mod pipeline;
mod rayon_all;

// pub use doc::OscarDoc;
pub use oscar_metadata::OscarMetadata;
// pub use rayon_all::RayonAll;
