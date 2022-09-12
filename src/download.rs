//!CommonCrawl asynchronous downloading.
//!
//! This module enables streaming (not tested yet) and one-shot downloading
//! of the CommonCrawl dataset.
//!
//! It only requires a `wet.paths` file that is available on CommonCrawl website.
use bytes::Bytes;
use futures::{stream, StreamExt};
use futures_core::stream::Stream;
use futures_util::TryStreamExt;
use log::Level;
use reqwest::{Client, Url};
use std::path::PathBuf;
use std::{
    io::{BufRead, BufReader},
    path::Path,
};
use tokio_util::compat::FuturesAsyncReadCompatExt;

/// Base url for commoncrawl downloading.
const BASE_URL: &str = "https://data.commoncrawl.org/";

#[derive(Debug)]
pub enum Error {
    Reqwest(reqwest::Error),
    Io(std::io::Error),
    Join(tokio::task::JoinError),
    Download(DownloadError),
}

/// wraps a reqwest::Error
/// with info about failed download,
/// namely destionation path and id
#[derive(Debug)]
pub struct DownloadError {
    pub err: reqwest::Error,
    pub path: PathBuf,
    pub id: usize,
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

/// async downloader of a single file.
///
/// Should not be used alone, as it is created by [Downloader].
struct Download<'a> {
    src: reqwest::Url,
    pub client: &'a reqwest::Client,
}

impl<'a> Download<'a> {
    /// asynchonously download and save to provided destination
    pub async fn save_to(&self, dst: &Path) -> Result<PathBuf, Error> {
        // get stream of bytes and convert into tokio-compatible reader
        let mut resp = self.stream().await?.into_async_read().compat();

        let mut file = tokio::fs::File::create(dst).await?;

        // copy bytes from response to file
        tokio::io::copy(&mut resp, &mut file).await?;
        info!("saved to {:?}", dst);
        Ok(PathBuf::from(dst))
    }

    /// get stream of bytes from request
    ///
    /// Streams fetched from this method are not tokio-compatible.
    /// See tokio-compat [example](https://github.com/benkay86/async-applied/tree/master/reqwest-tokio-compat)
    /// or [Self::save_to] sourcecode
    ///
    /// See [reqwest#482](https://github.com/seanmonstar/reqwest/issues/482) for more context.
    pub async fn stream(&self) -> Result<impl Stream<Item = futures::io::Result<Bytes>>, Error> {
        debug!("getting {}", self.src);
        let resp = self
            .client
            .get(self.src.clone())
            .send()
            .await?
            .error_for_status()?
            .bytes_stream()
            .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e));

        Ok(resp)
    }
}

/// async downloader that downloads numerous files from
/// a provided `wet.paths` file.
///
/// - [Downloader::urls] holds valid parsed urls from `wet/paths` file
/// - [Downloader::n_tasks] corresponds to the number of tasks spawned by [tokio].
pub struct Downloader {
    urls: Vec<reqwest::Url>,
    n_tasks: usize,
}

impl Downloader {
    /// Construct a vector of urls to download from
    /// from a wet.paths file
    // TODO: maybe rename? from is for type conversions.
    // TODO: watch for those `unwrap`.
    pub fn from_paths_file(
        paths_file: &std::fs::File,
        n_tasks: usize,
    ) -> Result<Self, std::io::Error> {
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
            error!("{:?}", failure.unwrap_err());
        }

        // in the same fashion, attempt to parse urls
        // and collect failures
        // unwrap() is deemed safe because we filtered failures previously
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
            error!("{:?}", failure.unwrap_err());
        }

        // unwrap successful paths
        let urls = urls.into_iter().map(Result::unwrap).collect();

        Ok(Downloader { urls, n_tasks })
    }

    /// launch downloading of urls
    ///
    /// See this [SO post](https://stackoverflow.com/questions/51044467/how-can-i-perform-parallel-asynchronous-http-get-requests-with-reqwest)
    /// for more info.
    pub async fn download(
        &mut self,
        dst: &Path,
        idx_offset: Option<usize>,
    ) -> Vec<Result<PathBuf, Error>> {
        // creates a new pathbuf that concats dst and i.gz
        let to_pathbuf = |i| {
            [dst, Path::new(&format!("{}.txt.gz", i))]
                .iter()
                .collect::<PathBuf>()
        };

        // skipping urls to offset
        let urls = if let Some(offset) = idx_offset {
            self.urls.iter().enumerate().skip(offset)
        } else {
            // we use skip(0) to have the same types
            // at if and else blocks.
            self.urls.iter().enumerate().skip(0)
        }
        .map(|(i, url)| (url, i, to_pathbuf(i)));

        let urls = stream::iter(urls);
        // create reqwests client.
        // this will be cloned for each task.
        let client = Client::new();

        let paths = urls
            .map(|(url, id, path)| {
                // clone client to use client pool
                // See https://github.com/seanmonstar/reqwest/issues/600
                // url to comply with 'static lifetime required by tokio
                // note: we could also use Arc?
                println!("Crawling {} to file {}.txt.gz", url, id);

                let client = client.clone();
                let url = url.clone();

                tokio::spawn(async move {
                    // launch download and return path or failure
                    let dl = Download {
                        src: url,
                        client: &client,
                    };

                    // wrap eventual Reqwest errors into DownloadErrors
                    // to add context
                    dl.save_to(&path).await.map_err(|e| match e {
                        Error::Reqwest(err) => Error::Download(DownloadError { err, path, id }),
                        _ => e,
                    })
                })
            })
            .buffer_unordered(self.n_tasks);

        // flatten nested errors
        paths.map(flatten_error).collect().await
    }
}

/// transforms a nested `Result<Result<PathBuf, Error>` into a `Result<PathBuf, Error>`.
fn flatten_error(
    e: Result<Result<PathBuf, Error>, tokio::task::JoinError>,
) -> Result<PathBuf, Error> {
    match e {
        Ok(e) => e,
        Err(e) => Err(Error::Join(e)),
    }
}
#[cfg(test)]
mod tests {

    use super::*;
    use sha1::Digest;
    use std::fs::File;
    use std::io::Read;
    #[tokio::test]
    #[ignore]
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

    #[tokio::test]
    #[ignore]
    pub async fn test_download_async_stream() {
        use tokio::fs::File;
        use tokio::io::AsyncReadExt;
        use tokio::io::AsyncWriteExt;

        let test_file_path = Path::new("tests/1Mio_async.dat");

        let client = reqwest::Client::new();
        let d = Download {
            src: reqwest::Url::parse("http://www.ovh.net/files/1Mio.dat")
                .expect("wrong url format"),
            client: &client,
        };

        let mut st = d.stream().await.unwrap();
        let mut file = File::create(test_file_path)
            .await
            .expect("failed to open file");

        while let Some(bytes) = st.next().await {
            file.write_all(&bytes.unwrap()).await.unwrap();
        }

        let mut hasher = sha1::Sha1::new();

        let mut file = File::open(test_file_path)
            .await
            .expect("could not open file");
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)
            .await
            .expect("could not read file");

        hasher.update(buf);

        let hash = hasher.finalize();
        assert_eq!(
            format!("{:x}", hash),
            "22c952ea2b497171d37b76f0830ef8d9911cfe9b".to_string()
        );

        std::fs::remove_file(test_file_path).expect("could not remove test file");
    }

    #[tokio::test]
    #[ignore]
    pub async fn test_downloader_init() {
        let valid_file_path = "tests/res/test.wet.paths";
        let f = File::open(&valid_file_path).expect("could not open");
        let d = Downloader::from_paths_file(&f, 4).expect("could not build downloader");

        assert_eq!(d.urls.len(), 4);
    }

    #[tokio::test]
    #[ignore]
    pub async fn test_downloader_init_invalid_url() {
        use std::io::Seek;
        use std::io::SeekFrom;
        let invalid_file_path = "tests/res/test.invalid.wet.paths";
        let mut f = File::open(&invalid_file_path).expect("could not open");
        let mut s = String::new();
        f.read_to_string(&mut s)
            .expect("could not read from wet file");
        f.seek(SeekFrom::Start(0))
            .expect("could not seek from wet file");
        assert_eq!(s.lines().count(), 4);
        let d = Downloader::from_paths_file(&f, 4).expect("could not build downloader");

        assert_eq!(d.urls.len(), 4);
    }

    #[tokio::test]
    #[ignore]
    pub async fn test_downloader_download() {
        let valid_file_path = "tests/res/test.wet.paths";
        let f = File::open(&valid_file_path).expect("could not open");
        let mut d = Downloader::from_paths_file(&f, 4).expect("could not build downloader");
        std::fs::create_dir("tests/dl").unwrap();
        d.download(Path::new("tests/dl"), Some(0)).await;
        assert!(false);
    }

    #[tokio::test]
    #[ignore]
    // test fails if test.wet.paths does not have 4 lines.
    pub async fn test_downloader_download_offset() {
        use std::collections::HashSet;

        let test_file_path = "tests/dl";
        let valid_file_path = "tests/res/test.wet.paths";
        let f = File::open(&valid_file_path).expect("could not open");
        let mut d = Downloader::from_paths_file(&f, 4).expect("could not build downloader");
        std::fs::create_dir(test_file_path).unwrap();
        d.download(Path::new(test_file_path), Some(2)).await;

        let mut expected_paths = HashSet::new();
        expected_paths.insert(PathBuf::from("tests/dl/2.gz"));
        expected_paths.insert(PathBuf::from("tests/dl/3.gz"));
        let paths: HashSet<PathBuf> = std::fs::read_dir(test_file_path)
            .unwrap()
            .map(|d| d.unwrap().path())
            .collect();
        println!("{:?}", paths);
        assert_eq!(paths, expected_paths);

        for path in paths {
            std::fs::remove_file(path).unwrap();
        }
        std::fs::remove_dir(test_file_path).unwrap();
    }
}
