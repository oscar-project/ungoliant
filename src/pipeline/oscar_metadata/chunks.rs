/*!
Utilities to transform chunks.

*/
use std::{collections::HashMap, ops::RangeInclusive};

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

#[cfg(test)]
mod tests {

    use super::*;
    use warc::header::WarcHeader;

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
}
