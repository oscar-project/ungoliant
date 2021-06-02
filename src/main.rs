use download::Downloader;
use std::fs::File;
use std::io::Write;
use structopt::StructOpt;

#[macro_use]
extern crate log;

mod classify;
mod cli;
mod download;
mod error;
mod lang;
mod metadata;
mod pipeline;
mod shard;

#[tokio::main]
async fn main() -> Result<(), error::Error> {
    env_logger::init();

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
                match failure.as_ref().unwrap_err() {
                    download::Error::Download(e) => {
                        write!(error_file, "{}\t{}", e.err.url().unwrap(), e.id)?;
                    }
                    _ => (),
                };
            }
        }

        cli::Ungoliant::Pipeline(p) => {
            let p = pipeline::OscarMetadata::new(p.src, p.dst);
            p.run()?;
        }
        _ => {
            unimplemented!();
        }
    };
    Ok(())
}
