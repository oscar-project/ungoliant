/*! Origin is a stamp that tells where a CommonCrawl document is located in a CCrawl dump.

Following document doc, each Piece contains **contiguous** lines, so we may have two entries with same metadata in a jsonl file.
!*/

use std::ops::RangeInclusive;

use serde::Deserialize;
use serde::Serialize;

use crate::error::Error;
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
/// Origin holds three values intended to enable fast retreival of a corpus document in CCrawl.
///
/// There three values are:
/// - `shard_number`: the shard number where the record is located
/// - `record_id`: the WARC record ID
/// - `range`: inclusive range (see [std::ops::RangeInclusive]) of lines extracted.
struct Origin {
    shard_number: u32,
    record_id: String,
    range: RangeInclusive<u32>,
}

impl Origin {
    /// Get headers for Origin csv export
    fn csv_headers() -> String {
        "shard_number,record_id,start,end".to_string()
    }

    /// get origin's shard number
    fn shard_number(&self) -> &u32 {
        &self.shard_number
    }

    /// get origin's record ID
    fn record_id(&self) -> &str {
        &self.record_id
    }

    /// get origin's range
    fn range(&self) -> &RangeInclusive<u32> {
        &self.range
    }

    /// Forge string containing origin data
    fn to_csv(&self) -> String {
        format!(
            "{},{},{},{}",
            self.shard_number,
            self.record_id,
            self.range.start(),
            self.range.end()
        )
    }

    /// get [Origin] from a comma-separated entry.
    /// headers are available in [Origin::csv_headers]
    fn from_csv(csv: &str) -> Result<Self, Error> {
        let mut parsed = csv.split(',');
        let shard_number: u32 = parsed.next().ok_or(Error::MalformedOrigin)?.parse()?;
        let record_id = parsed.next().ok_or(Error::MalformedOrigin)?.to_string();
        let range_start: u32 = parsed.next().ok_or(Error::MalformedOrigin)?.parse()?;
        let range_end: u32 = parsed.next().ok_or(Error::MalformedOrigin)?.parse()?;

        Ok(Origin {
            shard_number,
            record_id,
            range: range_start..=range_end,
        })
    }
}
#[cfg(test)]
mod tests {

    use super::*;

    fn gen_origin() -> Origin {
        let record = warc::Record::default().add_body("hi");
        let shard_number = 63_999u32;
        let range = 0..=10u32;
        let record_id = record.warc_id().to_string();

        Origin {
            shard_number,
            record_id,
            range,
        }
    }

    #[test]
    fn serialize() {
        let o = gen_origin();
        let serialized = serde_json::to_string(&o).unwrap();
        let result: Origin = serde_json::from_str(&serialized).unwrap();

        assert_eq!(o, result);
    }

    #[test]
    fn to_csv() {
        let o = gen_origin();

        let expected_record_id = o.record_id();
        let expected_range = o.range();
        let expected_shard_number = o.shard_number();
        let expected_csv = format!(
            "{},{},{},{}",
            expected_shard_number,
            expected_record_id,
            expected_range.start(),
            expected_range.end()
        );

        let o_csv = o.to_csv();

        assert_eq!(o_csv, expected_csv);
    }

    #[test]
    fn from_csv() {
        let record_id = gen_origin().record_id().to_string();
        let range = 0..=10;
        let shard_number = 10_999;

        let origin_csv = format!(
            "{},{},{},{}",
            shard_number,
            record_id,
            range.start(),
            range.end()
        );

        let expected = Origin {
            record_id,
            range,
            shard_number,
        };

        let result = Origin::from_csv(&origin_csv).unwrap();

        assert_eq!(expected, result);
    }
}
