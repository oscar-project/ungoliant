pub const SCHEMA_RECORD: &str = r#"{
        "type": "record",
        "name": "record_entry",
        "fields": [
            {"name": "record_id", "type": "string"},
            {"name": "corpus_offset_lines", "type": "long"},
            {"name": "nb_sentences", "type": "long"},
            {"name": "corpus_offset_bytes", "type": "long"},
            {"name": "shard_number", "type": "long"},
            {"name": "shard_record_number", "type": "long"}
        ]
    }"#;
pub const SCHEMA_RECORD_LIST: &str = r#"{
        "type": "array",
        "name": "record_list",
        "items": "record_entry"
    }"#;

pub const SCHEMA_WHOLE: &str = r#"{
        "type": "map",
        "values": "record_list",
        "name": "shard_entry"
    }"#;
