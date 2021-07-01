//! Rotating file writers for text and metadata.
use std::convert::TryFrom;
use std::fs::OpenOptions;
use std::path::Path;
use std::{fs::File, io::Write, path::PathBuf};

/// Rotating file writers.
///
/// Implement [std::io::Write] and holds a size (bytes) limit.
///
/// Note: if a slice to write is larger than the whole limit, then it is an expected behaviour that
/// the size limit is ignored and a file is created.
pub struct TextWriter {
    lang: &'static str,
    dst: PathBuf,
    text: Option<File>,
    size: u64,
    size_limit: u64,
    nb_files: u64,
}

impl TextWriter {
    /// Create a new [TextWriter].
    /// Note that nothing is created/written unless a write is performed.
    /// size_limit is in bytes.
    pub fn new(dst: &Path, lang: &'static str, size_limit: u64) -> Self {
        Self {
            lang,
            dst: dst.to_path_buf(),
            text: None,
            size: 0,
            size_limit,
            nb_files: 0,
        }
    }

    /// Rotate file.
    ///
    /// The first file is named `lang.txt`, and is renamed `lang_part_1.txt` if there's > 1 number of files.
    fn create_next_file(&mut self) -> std::io::Result<()> {
        let filename = if self.nb_files == 0 {
            format!("{}.txt", self.lang)
        } else {
            format!("{}_part_{}.txt", self.lang, self.nb_files + 1)
        };

        let mut path = self.dst.clone();
        path.push(filename);

        let mut options = OpenOptions::new();
        options.read(true).append(true).create(true);

        let text = options.open(path)?;

        //if nb_files == 1, rename lang.txt into lang_part_1.txt
        if self.nb_files == 1 {
            let mut from = self.dst.clone();
            from.push(format!("{}.txt", self.lang));
            let mut to = self.dst.clone();
            to.push(format!("{}_part_1.txt", self.lang));

            std::fs::rename(from, to)?;
        }

        self.text = Some(text);

        self.size = 0;
        self.nb_files += 1;
        Ok(())
    }
}

impl Write for TextWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // if there's no file open, create one
        if self.text.is_none() {
            self.create_next_file()?;
        }

        // if there's no space left on the current file, create another one
        if self.size + buf.len() as u64 > self.size_limit {
            self.create_next_file()?;
        }

        if let Some(text) = &mut self.text {
            let bytes_written = text.write(buf)?;
            self.size += match u64::try_from(bytes_written) {
                Ok(b) => b,
                Err(e) => {
                    error!(
                        "potential size overflow on lang {} file {} ({:?}): size set to {}",
                        self.lang, self.nb_files, e, self.size_limit
                    );
                    self.size_limit
                }
            };

            Ok(bytes_written)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "Could not write to file {} for lang {}",
                    self.nb_files, self.lang
                ),
            ))
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match &mut self.text {
            Some(text) => text.flush(),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;
    #[test]
    fn one_file() {
        std::fs::create_dir("tmp_one_file/").unwrap();
        let file_size = 10;
        let mut tw = TextWriter::new(&PathBuf::from("tmp_one_file/"), "en", file_size);
        let text = String::from("helloworld");

        assert_eq!(text.len() as u64, file_size);
        tw.write_all(text.as_bytes()).unwrap();

        let mut file = std::fs::File::open("tmp_one_file/en.txt").unwrap();
        let mut result = String::new();

        file.read_to_string(&mut result).unwrap();

        assert_eq!(text, result);
        assert_eq!(file_size, file.metadata().unwrap().len());

        std::fs::remove_dir_all("tmp_one_file/").unwrap();
    }

    #[test]
    fn multiple_files() {
        std::fs::create_dir("tmp_multiple/").unwrap();
        let file_size = 10;
        let mut tw = TextWriter::new(&PathBuf::from("tmp_multiple/"), "en", file_size);
        let text = String::from("helloworld");

        for i in 0..10 {
            tw.write_all(&text.as_bytes()).unwrap();
        }

        let mut b = String::new();
        for i in 1..=10 {
            b.clear();
            let filename = format!("tmp_multiple/en_part_{}.txt", i);
            let mut file = std::fs::File::open(&filename).unwrap();
            file.read_to_string(&mut b).unwrap();
            assert_eq!(b, text);
            assert_eq!(file_size, file.metadata().unwrap().len());
        }
        std::fs::remove_dir_all("tmp_multiple/").unwrap();
    }

    #[test]
    fn multiple_files_different_sizes() {
        std::fs::create_dir("tmp_multiple_sizes/").unwrap();
        let file_size = 10;
        let mut tw = TextWriter::new(&PathBuf::from("tmp_multiple_sizes/"), "en", file_size);
        let mut texts = vec![
            "helloworld\n",                 // fits in file 1
            "tiny\n",                       // fits in file 2
            "tiny\n",                       // fits in file 2
            "short\n",                      // fits in file 3
            "medium\n",                     // fits in file 4
            "laaaaaaaaaaaaaaaaaaaaaarge\n", // fits in file 5 even if it is too large
        ]
        .into_iter()
        .map(String::from);

        for _ in 0..5 {
            tw.write_all(&texts.next().unwrap().as_bytes()).unwrap();
        }

        let mut b = String::new();
        for i in 1..=4 {
            b.clear();
            let filename = format!("tmp_multiple_sizes/en_part_{}.txt", i);
            dbg!(&filename);
            let mut f = std::fs::File::open(&filename).unwrap();
            f.read_to_string(&mut b).unwrap();
            let f_size = f.metadata().unwrap().len() as u64;
            //TODO: find a way to validate.
            // f < filesize
            // but if origin text is > than filesize then it's ok if f > filesize.
            std::fs::remove_file(filename).unwrap();
        }
        std::fs::remove_dir_all("tmp_multiple_sizes/");
    }
}
