extern crate reqwest;

use std::io::BufReader;
use std::io::prelude::*;
use std::fs::File;

fn main() {
    let base_url = "";

    let f = File::open("").unwrap();
    let f = BufReader::new(f);
    let mut i = 0;

    for line in f.lines() {
        let line = line.unwrap();
        let target = format!("{}{}", base_url, line);
        let res = reqwest::blocking::get(&target).unwrap();
        println!("Crawling {:?}", line);

        let mut out = File::create(format!("result/{}.txt.gz", i.to_string())).expect("failed to create file");
        out.write(&res.bytes().unwrap()).unwrap();
        i += 1;
    }

}
