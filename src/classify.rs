use std::io::{Read, Write};
use std::process::{Child, Command, Stdio};

const MIN_SENTENCE_LEN: usize = 100;

/// ensure that sentences meet valid requirements
/// to be sent to fasttext:
/// - valid utf8: currently handled upper in the chain because strings can't be invalid utf8
/// - > 100 chars (go runes)
/// However, we're currently using from_utf8_lossy.
/// We have to use from_utf8 and catch failing strings
///
/// We also use chars(), that gives Unicode scalar values, not graphemes.
pub fn valid(sentence: &str) -> bool {
    // no checking in utf8 validity since 8
    sentence.chars().count() > MIN_SENTENCE_LEN
}

/// Used to identify language on strings
pub struct Classifier {
    process: Command,
    child: Option<Child>,
}

impl Classifier {
    /// Create a new fasttext command ready to be launched.
    /// Does *not* check if fastText is installed
    pub fn new() -> Self {
        let mut process = Command::new("fastText/fasttext");
        process
            .arg("predict-prob")
            .arg("fasttext/lid.176.bin")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped());

        Classifier {
            process,
            child: None,
        }
    }

    /// Run fasttext command that will wait for input
    pub fn spawn(&mut self) -> std::io::Result<()> {
        self.child = Some(self.process.spawn()?);
        Ok(())
    }

    pub fn predict(&mut self, str: &str) -> std::io::Result<Option<(String, f64)>> {
        let child = match &mut self.child {
            Some(ref child) => child,
            None => {
                self.spawn()?;
                self.child.as_ref().unwrap()
            }
        };


        let s = String::new();
        // match child.stdout.unwrap().read_to_string(&mut s) {
        //     Err(why) => panic!("couldn't read wc stdout: {}", why),
        //     Ok(_) => print!("wc responded with:\n{}", s),
        // }
        // match child.stdin.unwrap().write_all(str.as_bytes()) {
        //     Err(why) => panic!("couldn't write to wc stdin: {}", why),
        //     Ok(_) => println!("sent pangram to wc"),
        // }

        // let mut s = String::new();
        // match self.child.unwrap().stdout.unwrap().read_to_string(&mut s) {
        //     Err(why) => panic!("couldn't read wc stdout: {}", why),
        //     Ok(_) => print!("wc responded with:\n{}", s),
        // }

        Ok(Some((s, 1f64)))
    }
    /// Check if fasttext is on
    pub fn is_running(&self) -> bool {
        self.child.is_some()
    }
}
