use log::Level;
use reqwest::Url;
use std::io::{BufRead, BufReader};
use std::{fs::File, path::PathBuf};

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

/// holds urls to download and
/// http client that will make the requests.
pub struct Downloader {
    urls: Vec<reqwest::Url>,
    client: reqwest::blocking::Client,
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
