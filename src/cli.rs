use std::path::PathBuf;

use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "oscar-tools", about = "A collection of tools for OSCAR corpus")]
/// Holds every command that is callable by the `oscar-tools` command.
pub enum Ungoliant {
    #[structopt(about = "Downloading of CommonCrawl")]
    Download(Download),
    Pipeline(Pipeline),
}

#[derive(Debug, StructOpt)]
/// Download command and parameters.
// should it be merged with the "real" download one?
// or at least download::Downloader implement From<Download>
pub struct Download {
    #[structopt(parse(from_os_str), help = "path to wet.paths file")]
    pub paths_file: PathBuf,
    #[structopt(parse(from_os_str), help = "download destination")]
    pub dst: PathBuf,
    #[structopt(short = "t", help = "number of tokio tasks. Default is 4.")]
    pub n_tasks: Option<usize>,
    #[structopt(short = "o", help = "number of files to skip. Default is 0.")]
    pub offset: Option<usize>,
}

#[derive(Debug, StructOpt)]
pub struct Pipeline {
    #[structopt(parse(from_os_str), help = "source (contains n.txt.gz)")]
    pub src: PathBuf,
    #[structopt(parse(from_os_str), help = "pipeline result destination")]
    pub dst: PathBuf,
}
