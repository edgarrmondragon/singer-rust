use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use singer_rust::message::Message;

use crate::{singer_schema_to_arrow, Error, ToRecordBatch};

pub struct ParquetSink {
    schema: ArrowSchema,
    writer_properties: WriterProperties,
    output_path: PathBuf,
    batch_size: usize,
    current_batch: Vec<Message>,
}

impl ParquetSink {
    pub fn new(
        schema_message: &Message,
        base_dir: &PathBuf,
        batch_size: usize,
    ) -> Result<Self, Error> {
        let stream_name = match schema_message {
            Message::SCHEMA { stream, .. } => stream,
            _ => return Err(Error::Schema("Expected SCHEMA message".to_string())),
        };

        let schema = singer_schema_to_arrow(schema_message)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let output_path = base_dir.join(format!("{}.parquet", stream_name));

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
    fn test_parquet_sink() {
        let temp_dir = tempdir().unwrap();
        let base_dir = temp_dir.path().to_path_buf();

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

        let mut sink = ParquetSink::new(&schema_message, &base_dir, 2).unwrap();

        // Add some records
        sink.add_record(Message::RECORD {
            stream: "test".to_string(),
            record: json!({
                "id": "1",
                "name": "Alice"
            }),
            version: 1,
            time_extracted: None,
        })
        .unwrap();

        sink.add_record(Message::RECORD {
            stream: "test".to_string(),
            record: json!({
                "id": "2",
                "name": "Bob"
            }),
            version: 1,
            time_extracted: None,
        })
        .unwrap();

        sink.flush().unwrap();

        // Verify file exists and has content
        let output_path = base_dir.join("test.parquet");
        assert!(output_path.exists());
        assert!(output_path.metadata().unwrap().len() > 0);
    }
}
