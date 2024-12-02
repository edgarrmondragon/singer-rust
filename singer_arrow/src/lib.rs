use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use arrow_array::builder::StringBuilder;
use serde_json::Value;
use singer_rust::message::Message;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("Schema error: {0}")]
    Schema(String),
    #[error("Type conversion error: {0}")]
    TypeConversion(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
}

/// Convert JSON schema types to Arrow data types
pub fn json_type_to_arrow(type_obj: &Value) -> Result<DataType, Error> {
    let type_str = type_obj
        .as_str()
        .ok_or_else(|| Error::TypeConversion("Type must be a string".to_string()))?;

    match type_str {
        "string" => Ok(DataType::Utf8),
        "integer" => Ok(DataType::Int64),
        "number" => Ok(DataType::Float64),
        "boolean" => Ok(DataType::Boolean),
        // Handle more types and formats
        _ => Err(Error::TypeConversion(format!(
            "Unsupported type: {}",
            type_str
        ))),
    }
}

/// Convert Singer schema to Arrow schema
pub fn singer_schema_to_arrow(schema_msg: &Message) -> Result<ArrowSchema, Error> {
    match schema_msg {
        Message::SCHEMA { schema, .. } => {
            let properties = schema
                .get("properties")
                .ok_or_else(|| Error::Schema("Schema missing properties".to_string()))?;

            let fields: Result<Vec<Field>, Error> = properties
                .as_object()
                .ok_or_else(|| Error::Schema("Properties must be an object".to_string()))?
                .iter()
                .map(|(name, prop)| {
                    let type_value = prop
                        .get("type")
                        .ok_or_else(|| Error::Schema(format!("Property {} missing type", name)))?;
                    let data_type = json_type_to_arrow(type_value)?;
                    Ok(Field::new(name, data_type, false))
                })
                .collect();

            Ok(ArrowSchema::new(fields?))
        }
        _ => Err(Error::Schema("Not a SCHEMA message".to_string())),
    }
}

/// A trait for converting Singer records to Arrow record batches
pub trait ToRecordBatch {
    fn to_record_batch(&self, schema: &ArrowSchema) -> Result<RecordBatch, Error>;
}

impl ToRecordBatch for Vec<Message> {
    fn to_record_batch(&self, schema: &ArrowSchema) -> Result<RecordBatch, Error> {
        if self.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(schema.clone())));
        }

        let mut builders: HashMap<String, StringBuilder> = schema
            .fields()
            .iter()
            .map(|field| (field.name().clone(), StringBuilder::new()))
            .collect();

        // Convert records to columns
        for msg in self {
            if let Message::RECORD { record, .. } = msg {
                for (name, builder) in builders.iter_mut() {
                    let value = record.get(name);
                    match value {
                        Some(v) => builder.append_value(v.to_string()),
                        None => builder.append_null(),
                    }
                }
            }
        }

        // Finalize arrays
        let arrays: Result<Vec<ArrayRef>, Error> = schema
            .fields()
            .iter()
            .map(|field| {
                let mut builder = builders.remove(field.name()).unwrap();
                Ok(Arc::new(builder.finish()) as ArrayRef)
            })
            .collect();

        Ok(RecordBatch::try_new(Arc::new(schema.clone()), arrays?)?)
    }
}

pub mod target;
pub use target::ParquetTarget;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_json_type_to_arrow() {
        assert_eq!(
            json_type_to_arrow(&json!("string")).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            json_type_to_arrow(&json!("integer")).unwrap(),
            DataType::Int64
        );
        assert_eq!(
            json_type_to_arrow(&json!("number")).unwrap(),
            DataType::Float64
        );
    }

    #[test]
    fn test_record_to_batch() {
        let schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let records = vec![
            Message::RECORD {
                stream: "test".to_string(),
                record: json!({
                    "id": "1",
                    "name": "Alice"
                }),
                version: 1,
                time_extracted: None,
            },
            Message::RECORD {
                stream: "test".to_string(),
                record: json!({
                    "id": "2",
                    "name": "Bob"
                }),
                version: 1,
                time_extracted: None,
            },
        ];

        let batch = records.to_record_batch(&schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
    }
}
