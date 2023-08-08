//! Command line arguments and parameters management/parsing.
use std::path::PathBuf;

use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "ungoliant", about = "corpus generation tool.")]
/// Holds every command that is callable by the `oscar-tools` command.
pub enum Ungoliant {
    #[structopt(about = "Download a CommonCrawl release")]
    Download(Download),
    #[structopt(about = "Run pipeline")]
    Pipeline(Pipeline),
    // #[structopt(about = "Deduplicate a generated, not split corpus.")]
    // Dedup(Dedup),
    // #[structopt(about = "Split a not split corpus")]
    // Split(Split),
    // #[structopt(about = "Compress")]
    // Compress(Compress),
    // #[structopt(about = "package")]
    // Package(Package),
    #[structopt(about = "Rebuild the corpus for a given language.")]
    Rebuild(Rebuild),
    //#[structopt(about = "check for corpus validity. This is under construction and shouldn't be used. ")]
    //Check(Check),
}

#[derive(Debug, StructOpt)]
pub struct Check {
    #[structopt(parse(from_os_str), help = "Corpus file")]
    pub src: PathBuf,
    #[structopt(parse(from_os_str), help = "csv file destination")]
    pub dst: PathBuf,
}
#[derive(Debug, StructOpt)]
pub struct Rebuild {
    #[structopt(parse(from_os_str), help = "source rebuild file (not directory)")]
    pub src_rebuild: PathBuf,
    #[structopt(parse(from_os_str), help = "source shards directory")]
    pub src_shards: PathBuf,
    #[structopt(parse(from_os_str), help = "rebuild directory")]
    pub dst: PathBuf,
    #[structopt(help = "target language")]
    pub lang: String,
}
#[derive(Debug, StructOpt)]
/// Dedup command and parameters.
pub struct Dedup {
    #[structopt(parse(from_os_str), help = "source corpus location")]
    pub src: PathBuf,
    #[structopt(parse(from_os_str), help = "destination corpus location")]
    pub dst: PathBuf,
    #[structopt(
        help = "number of records in a bulk write.",
        long = "chunk_size",
        default_value = "500",
        short = "s"
    )]
    pub bufsize: usize,
}

#[derive(Debug, StructOpt)]
pub struct Compress {
    #[structopt(parse(from_os_str), help = "source corpus location")]
    pub src: PathBuf,
    #[structopt(parse(from_os_str), help = "destination corpus location")]
    pub dst: PathBuf,
}

#[derive(Debug, StructOpt)]
#[structopt(
    about = "Move files in language specific folders and compute checksums.
Using -m will move instead of copying. Not specifying a dst file will move in place."
)]
pub struct Package {
    #[structopt(parse(from_os_str), help = "source corpus location")]
    pub src: PathBuf,
    #[structopt(
        parse(from_os_str),
        help = "destination corpus location. Leave blank for in-place move."
    )]
    pub dst: Option<PathBuf>,
    #[structopt(short = "m", long = "move-files", help = "move files (no copy)")]
    pub move_files: bool,
}
#[derive(Debug, StructOpt)]
/// Dedup command and parameters.
pub struct Split {
    #[structopt(parse(from_os_str), help = "source corpus location")]
    pub src: PathBuf,
    #[structopt(parse(from_os_str), help = "destination corpus location")]
    pub dst: PathBuf,
    #[structopt(help = "size of each part (in MBytes)", default_value = "500")]
    pub part_size: u64,
    #[structopt(
        help = "number of records in a bulk write.",
        long = "chunk_size",
        default_value = "500",
        short = "s"
    )]
    pub bufsize: usize,
}

#[derive(Debug, StructOpt)]
/// Download command and parameters.
/// ```sh
/// ungoliant-download 0.1.0
/// Downloading of CommonCrawl
///
/// USAGE:
///     ungoliant download [OPTIONS] <paths-file> <dst>
///
/// FLAGS:
///     -h, --help       Prints help information
///     -V, --version    Prints version information
///
/// OPTIONS:
///     -t <n-tasks>        number of tokio tasks. Default is 4.
///     -o <offset>         number of files to skip. Default is 0.
///
/// ARGS:
///     <paths-file>    path to wet.paths file
///     <dst>           download destination
/// ```
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
/// Pipeline command and parameters.
///
/// ```sh
/// ungoliant-pipeline 0.1.0
/// Run pipeline
///
/// USAGE:
///     ungoliant pipeline [FLAGS] <src> <dst>
///
/// FLAGS:
///     -h, --help             Prints help information
///     -V, --version          Prints version information
///     -m, --with_metadata    extract metadata
///
/// ARGS:
///     <src>    source (contains n.txt.gz)
///     <dst>    pipeline result destination
/// ```
pub struct Pipeline {
    #[structopt(parse(from_os_str), help = "source (contains n.txt.gz)")]
    pub src: PathBuf,
    #[structopt(parse(from_os_str), help = "pipeline result destination")]
    pub dst: PathBuf,
    #[structopt(
        parse(from_os_str),
        long = "lid-path",
        help = "Path to 176.lid.bin",
        default_value = "lid.176.bin"
    )]
    pub lid_path: PathBuf,
    #[structopt(
        parse(from_os_str),
        long = "blocklist-path",
        help = "Optional path to blocklist."
    )]
    pub blocklist: Option<PathBuf>,

    #[structopt(
        parse(from_os_str),
        long = "domain-blocklists",
        help = "domain-blocklists path. For folders, will treat each file as a different blocklist. For files, filename=annotation. use ut1-blocklist for using ut1 blocklist annotations"
    )]
    pub domain_blocklists: Option<Vec<PathBuf>>,

    #[structopt(
        parse(from_os_str),
        long = "kenlms-path",
        help = "Optional path to kenlm folder. for the language xx, you have to have a xx.binary file."
    )]
    pub kenlms_path: Option<PathBuf>,

    #[structopt(
        help = "Split size (in MBytes). Default: No splitting",
        long = "split_size"
    )]
    pub split: Option<u64>,

    #[structopt(short = "c", long = "comp", help = "Enables zstd compression")]
    pub comp: bool,
}
