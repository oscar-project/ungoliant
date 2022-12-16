#![doc = include_str!("../README.md")]
use download::Downloader;
use log::LevelFilter;
use std::fs::File;
use std::io::Write;
use structopt::StructOpt;

use crate::pipelines::Pipeline;

#[macro_use]
extern crate log;

mod cli;
mod download;
mod error;
mod filtering;
mod identifiers;
mod io;
mod lang;
mod pipelines;
mod processing;
mod sources;
mod transformers;

#[tokio::main]
#[cfg(not(tarpaulin_include))]
async fn main() -> Result<(), error::Error> {
    // set devault log level to info
    let mut builder = env_logger::Builder::new();
    builder.filter_level(LevelFilter::Info);
    builder.parse_env("RUST_LOG");
    builder.init();

    let opt = cli::Ungoliant::from_args();
    debug!("cli args\n{:#?}", opt);

    match opt {
        cli::Ungoliant::Download(e) => {
            let paths = File::open(e.paths_file)?;
            let mut dl = Downloader::from_paths_file(&paths, e.n_tasks.unwrap_or(4))?;
            let results = dl.download(&e.dst, e.offset).await;

            let mut error_file = File::create("errors.txt")?;

            // write eventual download errors
            for failure in results.iter().filter(|result| result.is_err()) {
                error!("Error during download:\n {:?}", failure);
                // match failure.as_ref().unwrap_err() {
                //     download::Error::Download(e) => {
                //         write!(error_file, "{}\t{}", e.err.url().unwrap(), e.id)?;
                //     }
                //     _ => (),
                // };
                if let download::Error::Download(e) = failure.as_ref().unwrap_err() {
                    write!(error_file, "{}\t{}", e.err.url().unwrap(), e.id)?;
                }
            }
        }

        cli::Ungoliant::Pipeline(p) => {
            let mut schema_filepath = p.dst.clone();
            // let p = pipeline::OscarMetadata::new(p.src, p.dst, p.lid_path);
            let p = pipelines::OscarDocNew::new(
                p.src,
                p.dst,
                p.lid_path,
                p.blocklist,
                p.domain_blocklists,
                p.kenlms_path,
            );
            p.run()?;

            schema_filepath.push("metadata_schema.json");
            info!("creating json schema file {:?}", schema_filepath);
            let _f = File::create(schema_filepath)?;
            // f.write_all(Document::get_schema().unwrap().as_bytes())?;
            // f.write_all(Metadata::get_schema()?.as_bytes())?;
        }
        cli::Ungoliant::Dedup(d) => {
            processing::dedup::dedup(&d.src, &d.dst, Some(d.bufsize))?;
        }
        cli::Ungoliant::Split(s) => {
            processing::split::split(&s.src, &s.dst, s.part_size, Some(s.bufsize));
        }
        cli::Ungoliant::Compress(c) => {
            processing::compress::compress_corpus(&c.src, &c.dst)?;
        }
        cli::Ungoliant::Package(p) => {
            processing::package::package(&p.src, p.dst.as_deref(), p.move_files)?;
        }
        cli::Ungoliant::Rebuild(r) => {
            let l = r.lang.parse().expect("unexpected language");
            let rb = processing::rebuild::Rebuilder::new(&r.src_rebuild, &r.src_shards, &r.dst, l);
            rb.run()?;
        }
        cli::Ungoliant::Check(c) => processing::check::check(c.src, c.dst)?,
    };
    Ok(())
}
