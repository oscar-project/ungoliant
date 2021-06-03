/*!
Utilities to transform chunks.

*/
use std::{collections::HashMap, ops::RangeInclusive};

use crate::pipeline::oscar_metadata::Metadata;
use warc::header::WarcHeader;

/// Transforms a list of `values` into a list
/// of ranges of contiguous sequences of same values.
/// # Example
/// ```ignore
/// use std::collections::HashMap;
///
/// let values = vec![1, 1, 2, 2, 1, 3, 3];
/// let groups = group_by(values);
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

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn group_by_simple() {
        // simple case
        let langs = vec![
            "en", "en", //
            "fr", "fr", "fr", "fr", //
            "en", "en", //
            "fr", "fr", //
            "es", "es", "es", "es", //
        ];

        let mut expected: HashMap<&str, Vec<RangeInclusive<usize>>> = HashMap::new();
        expected.insert("en", vec![0..=1, 6..=7]);
        expected.insert("fr", vec![2..=5, 8..=9]);
        expected.insert("es", vec![10..=13]);

        let r = group_by(langs);
        println!("expected: {:?}", &expected);
        println!("result  : {:?}", &r);
        for (k, v) in r {
            assert_eq!(&v, expected.get(k).unwrap());
        }
    }

    #[test]
    fn group_by_empty() {
        let langs: Vec<&str> = Vec::new();

        let r = group_by(langs);
        assert!(r.is_empty());
    }

    #[test]
    fn group_by_uniq() {
        let langs = vec!["fr"; 10];

        let r = group_by(langs);
        let mut expected: HashMap<&str, Vec<RangeInclusive<usize>>> = HashMap::new();
        expected.insert("fr", vec![0..=9]);
        assert_eq!(r, expected);
    }

    #[test]
    fn group_by_uniq_but_first() {
        let mut langs = vec!["fr"; 10];
        langs.insert(0, "it");

        let r = group_by(langs);
        let mut expected: HashMap<&str, Vec<RangeInclusive<usize>>> = HashMap::new();
        expected.insert("it", vec![0..=0]);
        expected.insert("fr", vec![1..=10]);
        println!("{:?}", r);
        assert_eq!(r, expected);
    }
    #[test]
    fn group_by_uniq_but_last() {
        let mut langs = vec!["fr"; 10];
        langs.push("it");

        let r = group_by(langs);
        let mut expected: HashMap<&str, Vec<RangeInclusive<usize>>> = HashMap::new();
        expected.insert("fr", vec![0..=9]);
        expected.insert("it", vec![10..=10]);
        println!("{:?}", r);
        assert_eq!(r, expected);
    }

    fn gen_fr_chunk() -> (
        Vec<String>,
        &'static str,
        Vec<RangeInclusive<usize>>,
        HashMap<WarcHeader, Vec<u8>>,
    ) {
        let lang = "fr";
        let mut header = HashMap::new();

        // set a dummy warcheader
        header.insert(WarcHeader::ContentType, Vec::from("text/plain".as_bytes()));

        let sentences: Vec<String> = [
            "Bonjour et bienvenue",
            "Je suis une phrase de test",
            "Moi également",
        ]
        .iter()
        .map(|sentence| sentence.to_string())
        .collect();

        let ranges = vec![0..=sentences.len() - 1];

        (sentences, lang, ranges, header)
    }

    fn gen_en_chunk() -> (
        Vec<String>,
        &'static str,
        Vec<RangeInclusive<usize>>,
        HashMap<WarcHeader, Vec<u8>>,
    ) {
        let lang = "en";
        let mut header = HashMap::new();

        // set a dummy warcheader
        header.insert(WarcHeader::ContentType, Vec::from("text/plain".as_bytes()));

        let sentences: Vec<String> = ["Hello and welcome", "I'm a test sentence", "Hey, me too"]
            .iter()
            .map(|sentence| sentence.to_string())
            .collect();

        let ranges = vec![0..=sentences.len() - 1];

        (sentences, lang, ranges, header)
    }

    #[test]
    fn process_chunk_monolingual() {
        let mut offsets = HashMap::new();

        let (sentences, lang, ranges, header) = gen_fr_chunk();
        // bump shard offset to 10 in french
        offsets.insert(lang, 10);

        let string_expected: String = sentences.join("\n") + "\n";
        let lang_expected = "fr";
        let metadata_expected = Metadata {
            headers: vec![(WarcHeader::ContentType, "text/plain".to_string())]
                .into_iter()
                .collect(),
            offset: 10,
            nb_sentences: 3,
        };

        let ret = process_chunk(lang, &sentences, &header, ranges, &mut offsets);
        assert_eq!(ret.0, string_expected);
        assert_eq!(ret.1, lang_expected);
        assert_eq!(ret.2, metadata_expected);
        println!("{:?}", ret);
    }

    #[test]
    fn process_chunk_monolingual_multiple() {
        let mut offsets = HashMap::new();

        let chunks = vec![gen_fr_chunk(); 10];

        // bump shard offset to 10 in french
        offsets.insert("fr", 10);

        let mut ret = Vec::new();
        for (sentences, lang, ranges, header) in chunks {
            ret.push(process_chunk(
                lang,
                &sentences,
                &header,
                ranges,
                &mut offsets,
            ));
        }

        // check that:
        // - taking an offset and adding nb_sentences = next offset
        // - the total of summations equals the global offset.
        let total = ret.iter().fold(10, |acc, x| {
            assert_eq!(acc, x.2.offset);
            acc + x.2.nb_sentences
        });

        assert_eq!(offsets.get("fr"), Some(&total));
    }

    #[test]
    fn process_chunk_multilingual_multiple() {
        let mut offsets = HashMap::new();

        let mut chunks = vec![gen_fr_chunk(); 5];
        chunks.append(&mut vec![gen_en_chunk(); 5]);

        // bump shard offset to 10 in french
        let fake_offset_fr = 10;
        offsets.insert("fr", fake_offset_fr);

        // bump shard offset to 20 in english
        let fake_offset_en = 20;
        offsets.insert("en", fake_offset_en);

        let mut ret = Vec::new();
        for (sentences, lang, ranges, header) in chunks {
            ret.push(process_chunk(
                lang,
                &sentences,
                &header,
                ranges,
                &mut offsets,
            ));
        }

        // check that:
        // - taking an offset and adding nb_sentences = next offset
        // - the total of summations equals the global offset.
        // for each lang
        let total_fr =
            ret.iter()
                .filter(|(_, lang, _)| lang == &"fr")
                .fold(fake_offset_fr, |acc, x| {
                    assert_eq!(acc, x.2.offset);
                    acc + x.2.nb_sentences
                });

        let total_en =
            ret.iter()
                .filter(|(_, lang, _)| lang == &"en")
                .fold(fake_offset_en, |acc, x| {
                    assert_eq!(acc, x.2.offset);
                    acc + x.2.nb_sentences
                });

        assert_eq!(offsets.get("en"), Some(&total_en));
        assert_eq!(offsets.get("fr"), Some(&total_fr));
    }
}
