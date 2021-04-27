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
    pub paths_file: PathBuf,
    #[structopt(parse(from_os_str), help = "download destination")]
    pub dst: PathBuf,
    #[structopt(short = "t", help = "number of tokio tasks. Default is 4.")]
    pub n_tasks: Option<usize>,
    // #[structopt(short = "n", help = "number of files to fetch")]
    // n_files: Option<u32>,
}
