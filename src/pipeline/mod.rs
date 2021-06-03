//! Pipelines.
//!
//! Various pipelines are implemented here, and the module
//! provides a light [pipeline::Pipeline] trait that enables easy and flexible pipeline creation.
mod oscar_metadata;
pub mod pipeline;
mod rayon_all;
mod rayon_shard;

pub use oscar_metadata::metadata::Metadata;
pub use oscar_metadata::oscar_metadata::OscarMetadata;
pub use rayon_all::RayonAll;
pub use rayon_shard::RayonShard;
