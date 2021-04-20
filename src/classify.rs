use std::os::unix::io::AsRawFd;
use std::process::{Child, Command, Stdio};
use std::{
    io::{Read, Write},
    process::{ChildStdin, ChildStdout},
};

use itertools::Itertools;

use fasttext::FastText;
use crate::warc;

const MIN_SENTENCE_LEN: usize = 100;

/// ensure that sentences meet valid requirements
/// to be sent to fasttext:
/// - valid utf8: currently handled upper in the chain because strings can't be invalid utf8
/// - > 100 chars (go runes)
/// However, we're currently using from_utf8_lossy.
/// We have to use from_utf8 and catch failing strings
///
/// We also use chars(), that gives Unicode scalar values, not graphemes.
fn valid(sentence: &str) -> bool {
    // no checking in utf8 validity since 8
    sentence.chars().count() > MIN_SENTENCE_LEN
}

/// Used to identify language on strings
// Ensure that we respect that:
// https://doc.rust-lang.org/std/process/struct.Child.html#warning
pub struct Classifier {
    process: Command,
    child: Child,
    // stdin: ChildStdin,
    // stdout: ChildStdout,
}

impl Classifier {
    /// Create a new fasttext command ready to be launched.
    /// Does *not* check if fastText is installed
    pub fn new() -> std::io::Result<Self> {
        let mut process = Command::new("fastText/fasttext");
        process
            // .arg("predict-prob")
            // .arg("fastText/lid.176.bin")
            .arg("cat")
            .arg("-")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped());

        let mut child = process.spawn()?;
        // let stdin = child.stdin.take().unwrap();
        // let stdout = child.stdout.take().unwrap();
        Ok(Classifier {
            process,
            child,
            // stdin,
            // stdout
        })
    }

    /// Run fasttext command that will wait for input
    pub fn spawn(&mut self) -> std::io::Result<()> {
        self.child = self.process.spawn()?;
        Ok(())
    }

    pub fn predict(&mut self, record: &str) -> std::io::Result<Option<(String, f64)>> {
        warn!("Very ineffective!!");
        let mut child = self.process.spawn()?;
        debug!("{:?}", child.id());

        match child.stdin.take() {
            None => panic!("no stdin"),
            Some(mut stdin) => {
                let valid_lines = record.lines().filter(|line| valid(line));
                for line in valid_lines {
                    stdin.write_all(line.as_bytes())?;
                }
            }
        }

        let mut s = String::new();
        match child.stdout.take() {
            None => panic!("no stdout"),
            Some(mut stdout) => {
                s.clear();
                stdout.read_to_string(&mut s);
                println!("{:?}", s);
            }
        }

        child.wait();
        Ok(Some((("yes".to_string()), 1f64)))
    }

    pub fn predict_record(&mut self, record: &str) -> std::io::Result<Option<(String, f64)>> {
        // let stdin = self.child.stdin.take().unwrap();
        // let stdout = self.child.stdout.take().unwrap();
        let valid_lines = record.lines().filter(|line| valid(line)).join("\n");

        let mut stdout = self.child.stdout.as_mut().unwrap();
        let mut stdin = self.child.stdin.as_mut().unwrap();
        debug!("sending {:?}", &valid_lines);
        stdin.write_all(valid_lines.as_bytes())?;
        debug!("stdin {:?}", &stdin);
        let mut s = String::new();
        // stdout.read_to_string(&mut s)?;
        debug!("getting {:?}", &s);
        // {

        //     for line in valid_lines {
        //         println!("sending: {}", line);
        //         stdin.write_all(line.as_bytes())?;
        //     }

        //     // stdin.flush()?;
        //     let mut s = String::new();
        //     let mut buf = vec![0; 10];
        //     // stdout.read(&mut buf)?;
        //     stdout.read_to_string(&mut s)?;

        //     println!("read {:?}", s);
        //     s.clear();
        // }

        // {
        //     let mut stdout = self.child.stdout.as_mut().unwrap();
        //     let mut s = String::new();

        //     stdout.read_to_string(&mut s)?;

        //     println!("{}", &s);
        // }
        // let mut s = String::new();
        // match self.child.stdout.take().unwrap().read_to_string(&mut s) {
        //     Err(e) => panic!("uhh"),
        //     Ok(res) => println!("good")
        // };

        // println!("{}", s);

        Ok(Some((("yes".to_string()), 1f64)))
    }
}
