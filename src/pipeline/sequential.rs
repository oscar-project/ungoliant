use std::path::PathBuf;

use crate::{classify::Classifier, error::Error, lang::LangFiles, wet::Wet};
use warc::header::WarcHeader;

use super::pipeline::Pipeline;

struct Sequential {
    src: PathBuf,
    dst: PathBuf,
}

impl Pipeline<()> for Sequential {
    fn run(&self) -> Result<(), Error> {
        todo!();
        // let results = std::fs::read_dir(&self.src)?
        //     //TODO: log errors!
        //     //      using ok() silently discards errors
        //     .filter_map(|shard| shard.ok())
        //     .filter_map(|shard| Wet::from_path_gzip(&shard.path()).ok());

        // // convert to parallel iterator
        // // /!\: We use par_bridge, that is suboptimal
        // //      compared to implementing IntoParallelIterator
        // //      ourselves.
        // let results = results.enumerate();

        // // holds file handles
        // let langfiles = LangFiles::new(&self.dst)?;

        // // iterate over shards
        // results.for_each(|(idx, shard)| {
        //     let mut sorted_sentences = ShardContent::new();
        //     info!("processing shard {:?}", idx);

        //     // convert into a parallel iterator
        //     let wetfile = shard.enumerate().par_bridge();

        //     let shard_results: Vec<Vec<(String, &'static str)>> = wetfile
        //         .filter_map(|(idx_record, record)| match record {
        //             Ok(record) => RayonAll::process_record(record, &cls),
        //             Err(e) => {
        //                 warn!("Error on record {} of shard {}: {}", idx_record, idx, e);
        //                 return None;
        //             }
        //         })
        //         .collect(); //TODO: test with a for_each and a channel to send?

        //     // store predictions into sorted_sentences
        //     for record in shard_results {
        //         record
        //             .into_iter()
        //             .for_each(|(sentence, lang)| sorted_sentences.insert(sentence, lang));
        //     }

        //     // write to disk
        //     debug!("writing shard {:?} into lang files", idx);
        //     for (lang, sentences) in sorted_sentences.inner {
        //         let mut fd = langfiles.get(&lang).unwrap();
        //         let content = sentences.into_iter().join("\n");
        //         fd.write_all(&content.as_bytes()).unwrap();
        //     }
        // };
        // Ok(())
    }
}
