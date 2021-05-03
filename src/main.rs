use download::Downloader;
use itertools::Itertools;
use rayon::prelude::*;
use std::io::Write;
use std::{fs::File, path::PathBuf};
use structopt::StructOpt;
use warc::{RawRecord, header::WarcHeader};

extern crate fasttext;

#[macro_use]
extern crate log;

mod classify;
mod cli;
mod download;
mod pipeline;
mod wet;

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
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
            for file in std::fs::read_dir(p.src)? {
                let file = file?;
                println!("{:?}", &file);
                let records = wet::Wet::from_path_gzip(file.path()).expect("error opening warc");

                let lang_tag = WarcHeader::Unknown("warc-identified-content-language".to_string());
                for c in records.chunks(4).into_iter() {
                    let c : Vec<RawRecord> = c
                        .filter(Result::is_ok)
                        .map(Result::unwrap).collect();
                    
                    let c = c.par_iter().for_each(|record| {
                        match record.headers.get(&lang_tag) {
                            Some(lang) => println!("{:#?}", String::from_utf8_lossy(lang)),
                            None => (),
                        }
                    });
                }
                // for record in records {
                //     let record = record.unwrap();
                //     match record.headers.get(&lang_tag) {
                //         Some(lang) => println!("{:#?}", String::from_utf8_lossy(lang)),
                //         None => (),
                //     };
                // }
            }
            // println!("{:?}", std::fs::read_dir(p.src));
            unimplemented!();
        }
        _ => {
            unimplemented!();
        }
    };
    // let mut err_file = File::create("errors.txt").expect("failed to create error file");
    // let mut log_file = File::create("log.txt").expect("failed to create log file");

    // let warc_record = warc::Wet::from_path_gzip(opt.file)?;
    // let mut classifier = classify::Classifier::new_lid().expect("oops");

    // // FIX for robots: line
    // let mut warc_record = warc_record.into_iter().skip(1);
    // println!("{:?}", warc_record.next());

    // for record in warc_record {
    //     let record = record.expect("could not fetch record");
    //     let predictions: Vec<_> = record
    //         .lines()
    //         .filter(|line| classify::valid_len(line))
    //         .map(|line| (classifier.predict(line).unwrap_or(None), line))
    //         .filter(|pair| pair.0.is_some())
    //         .map(|pair| (pair.0.unwrap(), pair.1))
    //         .collect();

    //     for p in predictions {
    //         println!("{:?}", p);
    //     }
    // }
    // let d = Downloader::from_paths_file(&File::open(opt.file)?)?;

    // let results = d.download_all_blocking();

    // // print eventual errors
    // for error in results.iter().filter(|x| Result::is_err(x)) {
    //     eprintln!("{:?}", error);
    // }

    Ok(())
}
