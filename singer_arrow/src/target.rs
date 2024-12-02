use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use singer_rust::message::Message;

use crate::{singer_schema_to_arrow, Error, ToRecordBatch};

pub struct ParquetTarget {
    schema: ArrowSchema,
    writer_properties: WriterProperties,
    output_path: PathBuf,
    batch_size: usize,
    current_batch: Vec<Message>,
}

impl ParquetTarget {
    pub fn new(
        schema_message: &Message,
        output_path: PathBuf,
        batch_size: usize,
    ) -> Result<Self, Error> {
        let schema = singer_schema_to_arrow(schema_message)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        Ok(Self {
            schema,
            writer_properties: props,
            output_path,
            batch_size,
            current_batch: Vec::with_capacity(batch_size),
        })
    }

    pub fn add_record(&mut self, record: Message) -> Result<(), Error> {
        if let Message::RECORD { .. } = record {
            self.current_batch.push(record);

            if self.current_batch.len() >= self.batch_size {
                self.flush()?;
            }
        }

        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), Error> {
        if self.current_batch.is_empty() {
            return Ok(());
        }

        let batch = self.current_batch.to_record_batch(&self.schema)?;
        self.write_batch(&batch)?;
        self.current_batch.clear();

        Ok(())
    }

    fn write_batch(&self, batch: &RecordBatch) -> Result<(), Error> {
        let file = std::fs::File::create(&self.output_path)?;
        let mut writer = ArrowWriter::try_new(
            file,
            Arc::new(self.schema.clone()),
            Some(self.writer_properties.clone()),
        )?;

        writer.write(batch)?;
        writer.close()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    #[test]
    fn test_parquet_target() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().join("test.parquet");

        let schema_message = Message::SCHEMA {
            stream: "test".to_string(),
            schema: json!({
                "type": "object",
                "properties": {
                    "id": {
                        "type": "string"
                    },
                    "name": {
                        "type": "string"
                    }
                }
            }),
            key_properties: vec!["id".to_string()],
            bookmark_properties: vec![],
        };

        let mut target = ParquetTarget::new(&schema_message, output_path.clone(), 2).unwrap();

        // Add some records
        target
            .add_record(Message::RECORD {
                stream: "test".to_string(),
                record: json!({
                    "id": "1",
                    "name": "Alice"
                }),
                version: 1,
                time_extracted: None,
            })
            .unwrap();

        target
            .add_record(Message::RECORD {
                stream: "test".to_string(),
                record: json!({
                    "id": "2",
                    "name": "Bob"
                }),
                version: 1,
                time_extracted: None,
            })
            .unwrap();

        target.flush().unwrap();

        // Verify file exists and has content
        assert!(output_path.exists());
        assert!(output_path.metadata().unwrap().len() > 0);
    }
}
