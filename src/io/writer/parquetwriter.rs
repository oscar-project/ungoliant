#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use parquet::{
        file::{
            properties::WriterProperties,
            writer::{FileWriter, InMemoryWriteableCursor, SerializedFileWriter},
        },
        schema::parser::parse_message_type,
    };

    use crate::{identifiers::Identification, lang::Lang, pipelines::oscardoc::types::Document};

    struct DocumentParquetIterator<'a> {
        inner: &'a Document,
        part_nb: usize,
    }

    enum DocumentPart<'a> {
        Content(&'a String),
        Warc(&'a HashMap<String, String>),
        Annotation(&'a Option<Vec<String>>),
        Id(&'a Identification),
        LineIds(&'a [Option<Identification>]),
    }
    impl<'a> Iterator for DocumentParquetIterator<'a> {
        type Item = DocumentPart<'a>;

        fn next(&mut self) -> Option<Self::Item> {
            let ret = match self.part_nb {
                0 => Some(DocumentPart::Content(self.inner.content())),
                // 1 => Some(DocumentPart::Warc(self.inner.warc_headers())),
                2 => Some(DocumentPart::Annotation(self.inner.metadata().annotation())),
                3 => Some(DocumentPart::Id(self.inner.identification())),
                4 => Some(DocumentPart::LineIds(
                    self.inner.metadata().sentence_identifications(),
                )),
                _ => None,
            };
            self.part_nb += 1;
            ret
        }
    }
    impl Document {
        fn iter_parquet(&self) -> DocumentParquetIterator {
            DocumentParquetIterator {
                inner: &self,
                part_nb: 0,
            }
        }
    }
    #[test]
    fn test_simple() {
        let message_type = "
  message spark_schema {
    OPTIONAL BYTE_ARRAY a (UTF8);
    REQUIRED INT32 b;
    REQUIRED DOUBLE c;
    REQUIRED BOOLEAN d;
    OPTIONAL group e (LIST) {
      REPEATED group list {
        REQUIRED INT32 element;
      }
    }
  }
";

        let identification = "
    message identification {
        REQUIRED BYTE_ARRAY lang (UTF8);
        REQUIRED FLOAT id;
    }
        ";
        let document = "
        message document {
            REQUIRED BYTE_ARRAY content (UTF8);
            REQUIRED group warc_headers (MAP) {
                required binary header (UTF8);
                required binary value (UTF8);
            }
            required group metadata {
                required group identification {
                    required binary lang (UTF8);
                    required float id;
                }
                required group annotation (LIST) {
                    repeated group list {
                        optional binary element (UTF8);
                    }
                }
                required group sentence_identifications (LIST) {
                    repeated group list {
                        required binary lang (UTF8);
                        required float id;
                    }
                }
            }
        }
        ";
        let schema = Arc::new(parse_message_type(document).expect("Expected valid schema"));
        let props = Arc::new(WriterProperties::builder().build());
        let mut w = InMemoryWriteableCursor::default();
        println!("{:#?}", schema);
        let mut wr = SerializedFileWriter::new(w, schema, props).unwrap();
        let mut row_group_writer = wr.next_row_group().unwrap();
        while let Some(col_writer) = row_group_writer.next_column().unwrap() {
            let ids = vec![Identification::new(Lang::Fr, 1.0); 1000];
            match col_writer {
                parquet::column::writer::ColumnWriter::BoolColumnWriter(_) => println!("bool"),
                parquet::column::writer::ColumnWriter::Int32ColumnWriter(_) => println!("int32"),
                parquet::column::writer::ColumnWriter::Int64ColumnWriter(_) => println!("int64"),
                parquet::column::writer::ColumnWriter::Int96ColumnWriter(_) => println!("int96"),
                parquet::column::writer::ColumnWriter::FloatColumnWriter(_) => println!("float"),
                parquet::column::writer::ColumnWriter::DoubleColumnWriter(_) => println!("double"),
                parquet::column::writer::ColumnWriter::ByteArrayColumnWriter(_) => {
                    println!("bytearray")
                }
                parquet::column::writer::ColumnWriter::FixedLenByteArrayColumnWriter(_) => {
                    println!("fixedlenbytearray")
                }
            }

            row_group_writer.close_column(col_writer).unwrap();
        }
    }
}
