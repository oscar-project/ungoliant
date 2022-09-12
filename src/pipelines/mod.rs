//! Pipelines.
//!
//! Various pipelines are implemented here, and the module
//! provides a light [pipeline::Pipeline] trait that enables easy and flexible pipeline creation.
// pub mod oscardoc;
pub mod oscardoc;
pub mod oscarmeta;
pub mod oscartext;
#[allow(clippy::module_inception)]
pub mod pipeline;

// pub use oscardoc::Document;
// pub use oscardoc::Metadata;
// pub use oscardoc::OscarDoc;
pub use oscardoc::OscarDoc as OscarDocNew;
pub use oscarmeta::OscarMetadata;
pub use pipeline::Pipeline;
// pub use rayon_all::RayonAll;
