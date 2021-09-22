/*! Avro Schema of rebuild files

Each (avro) record is an  `(shard_id, array of (shard) records)`.
!*/
pub const SCHEMA: &str = r#"
    {
        "type": "record",
        "name": "shard_index",
        "fields": [
            {"name": "shard_id", "type": "long"},
            {
                "name": "records",
                "type": {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "record_entry",
                        "fields": [
             {"name": "record_id", "type": "string"},
             {"name": "corpus_offset_lines", "type": "long"},
             {"name": "nb_sentences", "type": "long"},
             {"name": "corpus_offset_bytes", "type": "long"},
             {"name": "start_hash", "type": "long"},
             {"name": "shard_number", "type": "long"},
             {"name": "shard_record_number", "type": "long"}
                        ]
                    }
                }
             }
        ]
 
     }
     "#;
