use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct BatchEncoding {
    /// The encoding of the batch
    format: String,

    /// The compression of the batch
    compression: String,
}

/// A Singer message
///
/// See the [Singer docs](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md#output).
///
/// # Examples
///
/// ```rust
/// use serde_json::{json, Value};
/// use singer_rust::message::Message;
///
/// let message = Message::RECORD {
///     stream: "my_stream".to_string(),
///     record: json!({"id": 1, "name": "John"}),
///     version: 1,
///     time_extracted: None,
/// };
/// ```
///
/// ```
/// use serde_json::json;
/// use singer_rust::message::Message;
///
/// let message = Message::STATE {
///     value: json!({"bookmarks": {"my_stream": {"version": 1}}}),
/// };
/// ```
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "type")]
pub enum Message {
    #[allow(non_camel_case_types)]
    ACTIVATE_VERSION {
        /// The name of the stream.
        stream: String,

        /// The version of the stream.
        version: u64,
    },
    BATCH {
        /// The name of the stream.
        stream: String,

        /// The manifest of the batch.
        manifest: Vec<String>,

        /// The encoding of the batch.
        encoding: BatchEncoding,
    },
    RECORD {
        /// The name of the stream.
        stream: String,

        /// The record to be written.
        record: Value,

        /// The version of the stream.
        time_extracted: Option<String>,

        /// The version of the stream.
        #[serde(default)]
        version: u64,
    },
    SCHEMA {
        /// The name of the stream.
        stream: String,

        /// The schema of the stream.
        schema: Value,

        /// The list of properties that are the primary keys for the stream.
        #[serde(default)]
        key_properties: Vec<String>,

        /// The list of properties that are the bookmarks for the stream.
        #[serde(default)]
        bookmark_properties: Vec<String>,
    },
    STATE {
        /// The state value.
        value: Value,
    },
}

impl Message {
    /// Convert a Singer message to a JSON string.
    ///
    /// # Arguments
    ///
    /// * `message` - A Singer message.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use serde_json::json;
    /// use singer_rust::message::Message;
    ///
    /// let message = Message::RECORD {
    ///   stream: "my_stream".to_string(),
    ///   record: json!({"id": 1, "name": "John"}),
    ///   version: 1,
    ///   time_extracted: None,
    /// };
    /// let json_string = message.to_string().unwrap();
    /// let parsed_message = Message::from_string(&json_string).unwrap();
    ///
    /// assert_eq!(message, parsed_message);
    pub fn to_string(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Convert a JSON string to a Singer message.
    ///
    /// # Arguments
    ///
    /// * `message` - A JSON string.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use serde_json::json;
    /// use singer_rust::message::Message;
    ///
    /// let message = r#"{
    ///   "type": "SCHEMA",
    ///   "stream": "my_stream",
    ///   "schema": {
    ///     "type": "object",
    ///     "properties": {
    ///       "id": {
    ///         "type": "integer"
    ///       }
    ///     }
    ///   },
    ///   "key_properties": ["id"]
    /// }"#;
    /// let parsed_message = Message::from_string(message).unwrap();
    ///
    /// assert_eq!(parsed_message, Message::SCHEMA {
    ///   stream: "my_stream".to_string(),
    ///   schema: json!({
    ///     "type": "object",
    ///     "properties": {
    ///       "id": {
    ///         "type": "integer"
    ///       }
    ///     }
    ///   }),
    ///   key_properties: vec!["id".to_string()],
    ///   bookmark_properties: vec![],
    /// });
    /// ```
    pub fn from_string(message: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(message)
    }
}
