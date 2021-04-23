use std::path::PathBuf;

use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "oscar-tools", about = "A collection of tools for OSCAR corpus")]
/// Holds every command that is callable by the `oscar-tools` command.
pub enum Ungoliant {
    #[structopt(about = "Downloading of CommonCrawl")]
    Download(Download),
}

#[derive(Debug, StructOpt)]
pub struct Download {
    #[structopt(parse(from_os_str), help = "path to wet.paths file")]
    pathfile: PathBuf,
    #[structopt(parse(from_os_str), help = "download destination")]
    dst: PathBuf,
    #[structopt(short = "J", help = "number of download threads. Default is 1")]
    n_threads: Option<u32>,
    #[structopt(short = "n", help = "number of files to fetch")]
    n_files: Option<u32>,
}
