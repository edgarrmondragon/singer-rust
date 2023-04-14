use std::io::{self, BufRead, BufReader, Read, Write};

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

    /// Process a stream of Singer messages.
    ///
    /// # Arguments
    ///
    /// * `reader` - A reader that implements `io::BufRead`.
    fn process_lines(&mut self, buffer: BufReader<impl Read>) -> Result<(), serde_json::Error> {
        for line in buffer.lines() {
            let line = line.expect("read input line");
            self.process_line(&line).expect("process input line");
        }
        Ok(())
    }

    /// Process a single Singer `RECORD` message.
    ///
    /// # Arguments
    ///
    /// * `stream` - The stream name.
    /// * `record` - An individual stream record.
    /// * `time_extracted` - Time the record was extracted.
    /// * `version` - Stream version for `FULL_TABLE` replication.
    fn process_record(
        &mut self,
        stream: String,
        record: serde_json::Value,
        time_extracted: Option<String>,
        version: u64,
    ) -> Result<(), serde_json::Error>;

    /// Process a single Singer `SCHEMA` message.
    ///
    /// # Arguments
    ///
    /// * `stream` - The stream name.
    /// * `schema` - The JSON schema of the stream.
    /// * `key_properties` - The primary key properties of the stream.
    /// * `bookmark_properties` - The replication key properties of the stream for `INCREMENTAL` replication.
    fn process_schema(
        &mut self,
        stream: String,
        schema: serde_json::Value,
        key_properties: Vec<String>,
        bookmark_properties: Vec<String>,
    ) -> Result<(), serde_json::Error>;

    /// Process a single Singer `STATE` message.
    ///
    /// # Arguments
    ///
    /// * `value` - The state payload.
    fn process_state(&mut self, value: serde_json::Value) -> Result<(), serde_json::Error>;

    /// Process a single Singer `ACTIVATE_VERSION` message.
    ///
    /// # Arguments
    ///
    /// * `stream` - The stream name.
    /// * `version` - The version of the stream.
    fn process_activate_version(
        &mut self,
        stream: String,
        version: u64,
    ) -> Result<(), serde_json::Error>;

    /// Process a single Singer `BATCH` message.
    ///
    /// # Arguments
    ///
    /// * `stream` - The stream name.
    /// * `manifest` - The manifest of the batch.
    /// * `encoding` - The encoding of the batch.
    fn process_batch(
        &mut self,
        stream: String,
        manifest: Vec<String>,
        encoding: BatchEncoding,
    ) -> Result<(), serde_json::Error>;
}
