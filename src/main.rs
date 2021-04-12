use std::{fs::File, path::{PathBuf}};
use std::io;
use std::io::prelude::*;
use std::io::BufReader;
use structopt::StructOpt;
//use flate2::bufread::MultiGzDecoder;

const BASE_URL: &str = "https://commoncrawl.s3.amazonaws.com/";

#[derive(Debug, StructOpt)]
#[structopt(name = "ungoliant", about = "A mysterious project named after a spider that consumes everything 🕷️.")]
struct UngoliantCli {
    #[structopt(help="paths to download, ending in wet.paths.")]
    file: PathBuf,
}

fn main() -> Result<(), std::io::Error> {

    let opt = UngoliantCli::from_args();

    let f = File::open(opt.file)?;
    let f = BufReader::new(f);
    let mut i = 0;

    let mut err_file = File::create("errors.txt").expect("failed to create error file");
    let mut log_file = File::create("log.txt").expect("failed to create log file");

    for line in f.lines() {
        let line = line.unwrap();
        let target = format!("{}{}", BASE_URL, line);

        let res = reqwest::blocking::get(&target);
        let res = match res {
            Ok(resp) => resp,
            Err(error) => {
                write!(
                    err_file,
                    "Problem downloading the file: {} => {}\n",
                    line, i
                )
                .unwrap();
                write!(err_file, "Error: {}\n", error).unwrap();
                i += 1;
                continue;
            }
        };
        println!("Crawling {} to file {}.txt.gz", line, i);
        write!(log_file, "Crawling {} to file {}.txt.gz\n", target, i).unwrap();

        let out = File::create(format!("result/{}.txt.gz", i.to_string()));
        let mut out = match out {
            Ok(file) => file,
            Err(error) => {
                write!(err_file, "Problem creating the file: {}.txt.gz\n", i).unwrap();
                write!(err_file, "Error: {}\n", error).unwrap();
                i += 1;
                continue;
            }
        };
        let mut buf = BufReader::new(res);
        match io::copy(&mut buf, &mut out) {
            Ok(_) => (),
            Err(error) => {
                write!(err_file, "Problem writing to file: {}.txt.gz\n", i).unwrap();
                write!(err_file, "Error: {}\n", error).unwrap();
                i += 1;
                continue;
            }
        };
        // let buf = BufReader::new(res);
        // let mut gz = MultiGzDecoder::new(buf);
        // io::copy(&mut gz, &mut out).unwrap();
        i += 1;
    }

    Ok(())
}
