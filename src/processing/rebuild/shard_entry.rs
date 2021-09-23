// use crate::processing::rebuild::rebuilder::BothLocation;
use serde::{Deserialize, Serialize};

use super::location::{Both, BothAvro};
/// List of relevant records (coded in [BothLocation])
/// per shard
#[derive(Debug)]
pub struct ShardEntry {
    shard_id: u64,
    records: Vec<Both>,
}

impl ShardEntry {
    pub fn new(shard_id: u64, records: Vec<Both>) -> Self {
        ShardEntry { shard_id, records }
    }
}
/// Avro-compatible version of [ShardEntry]. (u64 as i64)
#[derive(Debug, Deserialize, Serialize)]
pub struct ShardEntryAvro {
    shard_id: i64,
    records: Vec<BothAvro>,
}

impl From<ShardEntry> for ShardEntryAvro {
    fn from(s: ShardEntry) -> ShardEntryAvro {
        ShardEntryAvro {
            shard_id: s.shard_id as i64,
            records: s.records.into_iter().map(|b| b.into()).collect(),
        }
    }
}

impl From<ShardEntryAvro> for ShardEntry {
    fn from(s: ShardEntryAvro) -> ShardEntry {
        ShardEntry {
            shard_id: s.shard_id as u64,
            records: s.records.into_iter().map(|b| b.into()).collect(),
        }
    }
}

impl ShardEntry {
    /// Get a reference to the shard entry's records.
    pub fn records(&self) -> &[Both] {
        self.records.as_slice()
    }

    /// Get a reference to the shard entry's shard id.
    pub fn shard_id(&self) -> &u64 {
        &self.shard_id
    }
}
