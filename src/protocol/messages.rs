use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use crate::client::Protocol;

/// `SignalR` frame separator appended to serialized messages.
pub const RECORD_SEPARATOR: &str = "\u{001E}";

pub struct MessageParser;

impl MessageParser {
    pub fn serialize<T: ?Sized + Serialize>(
        value: &T,
        protocol: Protocol,
    ) -> Result<Vec<u8>, String> {
        match protocol {
            Protocol::Json => {
                let serialized =
                    serde_json::to_string(value).map_err(|e| format!("JSON error: {}", e))?;
                let mut bytes = serialized.into_bytes();
                bytes.extend_from_slice(RECORD_SEPARATOR.as_bytes());
                Ok(bytes)
            }
            Protocol::MessagePack => {
                let mut bytes =
                    rmp_serde::to_vec(value).map_err(|e| format!("MessagePack error: {}", e))?;
                bytes.extend_from_slice(RECORD_SEPARATOR.as_bytes());
                Ok(bytes)
            }
        }
    }

    pub fn deserialize<T: DeserializeOwned>(
        message: &[u8],
        protocol: Protocol,
    ) -> Result<T, String> {
        match protocol {
            Protocol::Json => {
                let text =
                    std::str::from_utf8(message).map_err(|e| format!("UTF-8 error: {}", e))?;
                serde_json::from_str::<T>(text).map_err(|e| e.to_string())
            }
            Protocol::MessagePack => rmp_serde::from_slice::<T>(message).map_err(|e| e.to_string()),
        }
    }

    // Legacy JSON-only methods for backward compatibility
    #[allow(dead_code)]
    pub fn to_json<T: ?Sized + Serialize>(value: &T) -> Result<String, serde_json::Error> {
        let serialized = serde_json::to_string(value)?;
        Ok(serialized + RECORD_SEPARATOR)
    }

    pub fn to_json_value<T: ?Sized + Serialize>(value: &T) -> Result<Value, serde_json::Error> {
        let serialized = serde_json::to_value(value)?;
        Ok(serialized)
    }

    pub fn strip_record_separator(input: &str) -> &str {
        input.trim_end_matches(RECORD_SEPARATOR)
    }

    pub fn parse_message<T: DeserializeOwned>(message: &str) -> Result<T, String> {
        serde_json::from_str::<T>(message).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::indexing_slicing, clippy::unwrap_used)]

    use super::*;
    use serde::Deserialize;
    use serde::Serialize;

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct TestMessage {
        text: String,
        number: i32,
    }

    #[test]
    fn test_to_json_adds_record_separator() {
        let msg = TestMessage {
            text: "hello".to_string(),
            number: 42,
        };
        let json = MessageParser::to_json(&msg).unwrap();
        assert!(json.ends_with(RECORD_SEPARATOR));
        assert!(json.contains("\"text\":\"hello\""));
        assert!(json.contains("\"number\":42"));
    }

    #[test]
    fn test_to_json_value() {
        let msg = TestMessage {
            text: "test".to_string(),
            number: 123,
        };
        let value = MessageParser::to_json_value(&msg).unwrap();
        assert!(value.is_object());
        assert_eq!(value["text"], "test");
        assert_eq!(value["number"], 123);
    }

    #[test]
    fn test_strip_record_separator() {
        let input = format!("test message{}", RECORD_SEPARATOR);
        let stripped = MessageParser::strip_record_separator(&input);
        assert_eq!(stripped, "test message");
        assert!(!stripped.contains(RECORD_SEPARATOR));
    }

    #[test]
    fn test_strip_record_separator_no_separator() {
        let input = "test message";
        let stripped = MessageParser::strip_record_separator(input);
        assert_eq!(stripped, "test message");
    }

    #[test]
    fn test_parse_message_success() {
        let json = r#"{"text":"hello","number":42}"#;
        let msg: TestMessage = MessageParser::parse_message(json).unwrap();
        assert_eq!(msg.text, "hello");
        assert_eq!(msg.number, 42);
    }

    #[test]
    fn test_parse_message_failure() {
        let json = r#"{"invalid":true}"#;
        let result: Result<TestMessage, String> = MessageParser::parse_message(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_round_trip() {
        let original = TestMessage {
            text: "round trip".to_string(),
            number: 999,
        };
        let json = MessageParser::to_json(&original).unwrap();
        let stripped = MessageParser::strip_record_separator(&json);
        let parsed: TestMessage = MessageParser::parse_message(stripped).unwrap();
        assert_eq!(parsed, original);
    }

    #[test]
    fn test_serialize_json() {
        let msg = TestMessage {
            text: "hello".to_string(),
            number: 42,
        };
        let bytes = MessageParser::serialize(&msg, Protocol::Json).unwrap();
        let text = String::from_utf8(bytes).unwrap();
        assert!(text.ends_with(RECORD_SEPARATOR));
        assert!(text.contains("\"text\":\"hello\""));
        assert!(text.contains("\"number\":42"));
    }

    #[test]
    fn test_serialize_messagepack() {
        let msg = TestMessage {
            text: "hello".to_string(),
            number: 42,
        };
        let bytes = MessageParser::serialize(&msg, Protocol::MessagePack).unwrap();
        // Should end with record separator
        assert!(bytes.ends_with(RECORD_SEPARATOR.as_bytes()));
        // Should be binary data (not text)
        assert!(bytes.len() < 100); // MessagePack should be compact
    }

    #[test]
    fn test_deserialize_json() {
        let json = r#"{"text":"hello","number":42}"#;
        let msg: TestMessage = MessageParser::deserialize(json.as_bytes(), Protocol::Json).unwrap();
        assert_eq!(msg.text, "hello");
        assert_eq!(msg.number, 42);
    }

    #[test]
    fn test_deserialize_messagepack() {
        let original = TestMessage {
            text: "hello".to_string(),
            number: 42,
        };
        // First serialize to MessagePack
        let bytes = MessageParser::serialize(&original, Protocol::MessagePack).unwrap();
        // Strip record separator
        let data = &bytes[..bytes.len() - RECORD_SEPARATOR.len()];
        // Then deserialize
        let msg: TestMessage = MessageParser::deserialize(data, Protocol::MessagePack).unwrap();
        assert_eq!(msg.text, "hello");
        assert_eq!(msg.number, 42);
    }

    #[test]
    fn test_round_trip_json() {
        let original = TestMessage {
            text: "round trip json".to_string(),
            number: 123,
        };
        let bytes = MessageParser::serialize(&original, Protocol::Json).unwrap();
        let data = &bytes[..bytes.len() - RECORD_SEPARATOR.len()];
        let parsed: TestMessage = MessageParser::deserialize(data, Protocol::Json).unwrap();
        assert_eq!(parsed, original);
    }

    #[test]
    fn test_round_trip_messagepack() {
        let original = TestMessage {
            text: "round trip msgpack".to_string(),
            number: 456,
        };
        let bytes = MessageParser::serialize(&original, Protocol::MessagePack).unwrap();
        let data = &bytes[..bytes.len() - RECORD_SEPARATOR.len()];
        let parsed: TestMessage = MessageParser::deserialize(data, Protocol::MessagePack).unwrap();
        assert_eq!(parsed, original);
    }
}
