//! Utilities to transform chunks.
//!
use std::{collections::HashMap, ops::RangeInclusive};

use crate::pipeline::oscar_metadata::Metadata;
use warc::header::WarcHeader;

/// Transforms a list of `values` into a list
/// of ranges of contiguous sequences of same values.
/// # Example
/// ```
/// let values = vec![1, 1, 2, 2, 1, 3, 3];
/// let groups = Pipeline::group_by(values);
/// let mut expected = HashMap::new();
/// expected.insert(1, vec![0..=1, 4..=4]);
/// expected.insert(2, vec![2..=3]);
/// expected.insert(3, vec![5..=6]);
/// assert_eq!(groups, expected);
/// ```
// todo: remove copy requirement
pub fn group_by<T: Eq + std::hash::Hash + Copy>(
    vec: Vec<T>,
) -> HashMap<T, Vec<RangeInclusive<usize>>> {
    let nb_sentences = vec.len();
    let mut block_start = 0;
    let mut block_end;
    let mut cur_group = None;
    let mut ret: HashMap<T, Vec<RangeInclusive<usize>>> = HashMap::new();

    //early return if there's no element
    if nb_sentences == 0 {
        return ret;
    }
    //early return if there's only one element
    if nb_sentences == 1 {
        ret.insert(vec[0], vec![0..=0]);
        return ret;
    }

    // iterate into items from vector
    for (idx, item) in vec.into_iter().enumerate() {
        // see if we've already initiated a chunk
        match cur_group {
            // start first chunk
            None => {
                block_start = idx;
                cur_group = Some(item);
            }
            Some(group) => {
                // if item is not of the same value of group
                // close current chunk and open another
                if item != group {
                    block_end = idx - 1;
                    let chunk = block_start..=block_end;
                    // insert or create vec holding chunks
                    // of said language
                    match ret.get_mut(&group) {
                        Some(chunks) => chunks.push(chunk),
                        None => {
                            ret.insert(group, vec![chunk]);
                        }
                    }

                    // set chunk start offset
                    // and current language
                    block_start = idx;
                    cur_group = Some(item);
                }
            }
        }
    }

    // close last chunk
    block_end = nb_sentences - 1;
    let chunk = block_start..=block_end;
    match cur_group {
        None => println!("???"),
        Some(group) => match ret.get_mut(&group) {
            Some(chunks) => chunks.push(chunk),
            None => {
                ret.insert(group, vec![chunk]);
            }
        },
    }
    ret
}

/// takes a chunk (lang, sentences, header, ranges)
/// computes a unique string from sentences and
/// creates a [metadata::Metadata] struct with
/// shard-local offsets.
///
/// Returns the unique string, the language and Metadata.
pub fn process_chunk(
    lang: &'static str,
    sentences: &[String],
    header: &HashMap<WarcHeader, Vec<u8>>,
    ranges: Vec<RangeInclusive<usize>>,
    offsets: &mut HashMap<&'static str, usize>,
) -> (String, &'static str, Metadata) {
    // sums ranges for each identified language
    // this way we know which offset to provide for next iteration
    let nb_sentences = ranges
        .iter()
        .fold(0, |acc, x| acc + x.end() - x.start() + 1);

    // register/bump offsets
    // and return starting offset of content
    let offset: usize = match offsets.get_mut(lang) {
        Some(off) => {
            *off += nb_sentences;
            *off - nb_sentences
        }
        None => {
            offsets.insert(lang, nb_sentences);
            0
        }
    };

    // concat sentence
    let mut sen = String::new();
    for range in ranges {
        sen += &sentences[range].join("\n");
        sen += "\n";
    }

    //convert u8 into Strings
    let header_str: HashMap<WarcHeader, String> = header
        .iter()
        .map(|(k, v)| (k.clone(), String::from_utf8_lossy(v).to_string()))
        .collect();

    let meta = Metadata {
        headers: header_str,
        offset,
        nb_sentences,
    };

    (sen, lang, meta)
}
