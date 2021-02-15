//extern crate reqwest;
extern crate flate2;

use std::io;
use std::io::BufReader;
use std::io::prelude::*;
use std::fs::File;
use flate2::bufread::MultiGzDecoder;

fn main() {
    let base_url = "https://commoncrawl.s3.amazonaws.com/";

    let f = File::open("test.wet.paths").unwrap();
    let f = BufReader::new(f);
    let mut i = 0;

    for line in f.lines() {
        let line = line.unwrap();
        let target = format!("{}{}", base_url, line);
        let res = reqwest::blocking::get(&target).unwrap();
        println!("Crawling {:?}", line);

        

        let mut out = File::create(format!("result/{}.txt", i.to_string())).expect("failed to create file");
        //res.copy_to(&mut out).unwrap();
        let buf = BufReader::new(res);
        let mut gz = MultiGzDecoder::new(buf);
        io::copy(&mut gz, &mut out).unwrap();
        i += 1;
    }
}
