use std::io;
use std::io::BufReader;
use std::io::prelude::*;
use std::fs::File;
//use flate2::bufread::MultiGzDecoder;

fn main() {
    let base_url = "https://commoncrawl.s3.amazonaws.com/";

    let f = File::open("test.wet.paths").unwrap();
    let f = BufReader::new(f);
    let mut i = 0;

    let mut err_file = File::create("errors.txt").expect("failed to create error file");
    let mut log_file = File::create("log.txt").expect("failed to create log file");

    for line in f.lines() {
        let line = line.unwrap();
        let target = format!("{}{}", base_url, line);

        let res = reqwest::blocking::get(&target);
        let res = match res {
            Ok(resp) => resp,
            Err(error) => {
                write!(err_file, "Problem downloading the file: {} => {}\n", line, i).unwrap();
                write!(err_file, "Error: {}\n", error).unwrap();
                i += 1;
                continue;
            },
        };
        println!("Crawling {} to file {}.txt.gz", line, i);
        write!(log_file, "Crawling {} to file {}.txt.gz\n", target, i).unwrap();

        let out = File::create(format!("result/{}.txt.gz", i.to_string()));
        let mut out = match out{
            Ok(file) => file,
            Err(error) => {
                write!(err_file, "Problem creating the file: {}.txt.gz\n", i).unwrap();
                write!(err_file, "Error: {}\n", error).unwrap();
                i += 1;
                continue;
            },
        };
        let mut buf = BufReader::new(res);
        match io::copy(&mut buf, &mut out) {
            Ok(_) => (),
            Err(error) => {
                write!(err_file, "Problem writing to file: {}.txt.gz\n", i).unwrap();
                write!(err_file, "Error: {}\n", error).unwrap();
                i += 1;
                continue;
            },
        };
        // let buf = BufReader::new(res);
        // let mut gz = MultiGzDecoder::new(buf);
        // io::copy(&mut gz, &mut out).unwrap();
        i += 1;
    }
}
