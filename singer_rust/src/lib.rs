pub use message::{write_message, BatchEncoding, Message, MessageReader};

pub mod message;

#[cfg(test)]
mod tests {
    use serde_json::{json, Error};

    use super::message;

    #[test]
    fn test_write_message() {
        let message = message::Message::RECORD {
            stream: "my_stream".to_string(),
            record: json!({"id": 1, "name": "John"}),
            version: 1,
            time_extracted: None,
        };
        match message::write_message(&message) {
            Ok(_) => (),
            Err(e) => panic!("Failed to write message: {}", e),
        }
    }

    #[test]
    fn test_parse_schema() {
        let message = r#"{
            "type": "SCHEMA",
            "stream": "my_stream",
            "schema": {
                "type": "object",
                "properties": {
                    "id": {
                        "type": "integer"
                    }
                }
            },
            "key_properties": ["id"]
        }"#;
        match message::Message::from_string(message) {
            Ok(message::Message::SCHEMA {
                stream,
                key_properties,
                bookmark_properties,
                schema,
                ..
            }) => {
                assert_eq!(stream, "my_stream".to_string());
                assert_eq!(key_properties, vec!["id".to_string()]);
                assert_eq!(bookmark_properties, Vec::<String>::new());
                assert_eq!(
                    schema,
                    json!({
                        "type": "object",
                        "properties": {
                            "id": {
                                "type": "integer"
                            }
                        }
                    })
                );
            }
            _ => panic!("Failed to parse message"),
        }
    }

    #[test]
    fn test_parse_record() {
        let message = r#"{
            "type": "RECORD",
            "stream": "my_stream",
            "record": {
                "id": 1,
                "name": "John"
            },
            "version": 1,
            "time_extracted": "2020-01-01T00:00:00Z"
        }"#;
        match message::Message::from_string(message) {
            Ok(message::Message::RECORD {
                stream,
                record,
                version,
                time_extracted,
            }) => {
                assert_eq!(stream, "my_stream".to_string());
                assert_eq!(
                    record,
                    json!({
                        "id": 1,
                        "name": "John"
                    })
                );
                assert_eq!(version, 1);
                assert_eq!(time_extracted, Some("2020-01-01T00:00:00Z".to_string()));
            }
            _ => panic!("Failed to parse message"),
        }
    }

    #[test]
    fn test_parse_activate_version() {
        let message = r#"{
            "type": "ACTIVATE_VERSION",
            "stream": "my_stream",
            "version": 1
        }"#;
        match message::Message::from_string(message) {
            Ok(message::Message::ACTIVATE_VERSION { stream, version }) => {
                assert_eq!(stream, "my_stream".to_string());
                assert_eq!(version, 1);
            }
            _ => panic!("Failed to parse message"),
        }
    }

    #[test]
    fn test_parse_state() {
        let message = r#"{
            "type": "STATE",
            "value": {
                "bookmarks": {
                    "my_stream": {
                        "version": 1
                    }
                }
            }
        }"#;
        match message::Message::from_string(message) {
            Ok(message::Message::STATE { value }) => {
                assert_eq!(
                    value,
                    json!({
                        "bookmarks": {
                            "my_stream": {
                                "version": 1
                            }
                        }
                    })
                );
            }
            _ => panic!("Failed to parse message"),
        }
    }

    #[test]
    fn test_parse_unknown() {
        let message = r#"{
            "type": "UNKNOWN",
            "value": "unknown"
        }"#;
        match message::Message::from_string(message) {
            Result::Err(Error { .. }) => {}
            _ => panic!("Should have failed to parse message"),
        }
    }
}
