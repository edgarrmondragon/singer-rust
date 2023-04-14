use std::io::{self, Write};

use serde_json;

use super::{BatchEncoding, Message};

/// Write a Singer message to stdout.
///
/// # Arguments
///
/// * `message` - A Singer message.
///
/// # Examples
///
/// ```rust
/// use serde_json::json;
/// use singer_rust::message::{Message, write_message};
///
/// let message = Message::RECORD {
///     stream: "my_stream".to_string(),
///     record: json!({"id": 1, "name": "John"}),
///     version: 1,
///     time_extracted: None,
/// };
/// write_message(&message).unwrap();
/// ```
pub fn write_message(message: &Message) -> io::Result<()> {
    let mut stdout = io::stdout().lock();
    serde_json::to_writer(&mut stdout, message).expect("Failed to serialize message");
    stdout.write_all(b"\n")?;
    Ok(())
}

pub trait MessageReader {
    fn process_line(&mut self, line: &str) -> Result<(), serde_json::Error> {
        let message: Message = serde_json::from_str(line)?;
        match message {
            Message::RECORD {
                stream,
                record,
                version,
                time_extracted,
            } => self.process_record(stream, record, time_extracted, version),
            Message::SCHEMA {
                stream,
                schema,
                key_properties,
                bookmark_properties,
            } => self.process_schema(stream, schema, key_properties, bookmark_properties),
            Message::STATE { value } => self.process_state(value),
            Message::ACTIVATE_VERSION { stream, version } => {
                self.process_activate_version(stream, version)
            }
            Message::BATCH {
                stream,
                manifest,
                encoding,
            } => self.process_batch(stream, manifest, encoding),
        }
    }

    fn process_record(
        &mut self,
        stream: String,
        record: serde_json::Value,
        time_extracted: Option<String>,
        version: u64,
    ) -> Result<(), serde_json::Error>;

    fn process_schema(
        &mut self,
        stream: String,
        schema: serde_json::Value,
        key_properties: Vec<String>,
        bookmark_properties: Vec<String>,
    ) -> Result<(), serde_json::Error>;

    fn process_state(&mut self, value: serde_json::Value) -> Result<(), serde_json::Error>;

    fn process_activate_version(
        &mut self,
        stream: String,
        version: u64,
    ) -> Result<(), serde_json::Error>;

    fn process_batch(
        &mut self,
        stream: String,
        manifest: Vec<String>,
        encoding: BatchEncoding,
    ) -> Result<(), serde_json::Error>;
}
