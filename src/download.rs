use log::Level;
use reqwest::Url;
use std::{fs::File, io::Read, path::PathBuf, stream::Stream};
use std::{
    io::{BufRead, BufReader},
    path::Path,
};

use bytes::Buf;

const BASE_URL: &str = "https://commoncrawl.s3.amazonaws.com/";

#[derive(Debug)]
pub enum Error {
    Reqwest(reqwest::Error),
    Io(std::io::Error),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err)
    }
}

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        Error::Reqwest(err)
    }
}

pub struct Download<'a> {
    src: reqwest::Url,
    client: &'a reqwest::Client,
}

impl<'a> Download<'a> {
    pub async fn save_to(&self, dst: &Path) -> Result<(), Error> {
        let resp = self
            .client
            .get(self.src.clone())
            .send()
            .await?
            .bytes()
            .await?;
        let mut file = File::create(dst)?;

        std::io::copy(&mut resp.reader(), &mut file);

        Ok(())
    }

    pub async fn stream(&self) -> Result<(), Error> {
        let resp = self
            .client
            .get(self.src.clone())
            .send()
            .await?
            .bytes_stream();

        Ok(())
    }
}
/// holds urls to download and
/// http client that will make the requests.
pub struct Downloader {
    urls: Vec<reqwest::Url>,
    client: reqwest::blocking::Client,
}

impl From<crate::cli::Download> for Downloader {
    fn from(d: crate::cli::Download) -> Self {
        todo!();
    }
}

impl Downloader {
    /// Construct a vector of urls to download from
    /// from a .paths file
    /// TODO: maybe rename? from is for type conversions.
    pub fn from_paths_file(paths_file: &std::fs::File) -> Result<Self, std::io::Error> {
        debug!("Downloader using {:#?}", paths_file);
        let f = BufReader::new(paths_file);

        // get all lines and partition by result state
        let (urls, failures): (Vec<_>, Vec<_>) = f.lines().partition(Result::is_ok);

        if log_enabled!(Level::Debug) {
            debug!(
                "Got {valid}/{total} valid lines",
                valid = urls.len(),
                total = urls.len() + failures.len()
            )
        }

        //print failed lines
        for failure in failures {
            eprintln!("{:?}", failure.unwrap_err());
        }

        // in the same fashion, attempt to parse urls
        // and collect failures
        let (urls, failures): (Vec<_>, Vec<_>) = urls
            .into_iter()
            .map(|link| Url::parse(&format!("{}{}", BASE_URL, link.unwrap())))
            .partition(Result::is_ok);

        if log_enabled!(Level::Debug) {
            debug!(
                "Got {valid}/{total} valid URLs",
                valid = urls.len(),
                total = urls.len() + failures.len()
            )
        }

        // print failures
        for failure in failures {
            eprintln!("{:?}", failure.unwrap_err());
        }

        // unwrap successful paths
        let urls = urls.into_iter().map(Result::unwrap).collect();

        Ok(Downloader {
            urls,
            client: reqwest::blocking::Client::new(),
        })
    }

    /// attempt to download from `url`, storing the result in result/`id`.txt
    fn download_blocking(&self, url: &Url, id: usize) -> Result<PathBuf, Error> {
        //fire blocking request, create out file,
        //load content into buffer and copy buffer into file.
        debug!("downloading {}", &url);
        let response = self.client.get(url.clone()).send()?;
        let path: PathBuf = PathBuf::from(format!("result/{}.txt.gz", id));
        let mut out = File::create(&path)?;
        let mut buf = BufReader::new(response);
        std::io::copy(&mut buf, &mut out)?;

        Ok(path)
    }

    /// sequentially download paths
    pub fn download_all_blocking(&self) -> Vec<Result<PathBuf, Error>> {
        let nb_links = self.urls.len();
        self.urls
            .iter()
            .enumerate()
            .map(|(id, url)| {
                println!("downloading {}/{}", id + 1, nb_links);
                self.download_blocking(url, id)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha1::Digest;
    #[tokio::test]
    pub async fn test_download_async() {
        let test_file_path = Path::new("tests/1Mio.dat");

        let client = reqwest::Client::new();
        let d = Download {
            src: reqwest::Url::parse("http://www.ovh.net/files/1Mio.dat")
                .expect("wrong url format"),
            client: &client,
        };

        d.save_to(test_file_path)
            .await
            .expect("could not download test file");

        let mut hasher = sha1::Sha1::new();

        let mut file = File::open(test_file_path).expect("could not open file");
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).expect("could not read file");

        hasher.update(buf);

        let hash = hasher.finalize();
        assert_eq!(
            format!("{:x}", hash),
            "22c952ea2b497171d37b76f0830ef8d9911cfe9b".to_string()
        );

        std::fs::remove_file(test_file_path).expect("could not remove test file");
    }
}
