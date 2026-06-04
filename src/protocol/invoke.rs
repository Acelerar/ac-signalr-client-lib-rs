use super::messages::MessageParser;
use super::negotiate::MessageType;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Invocation {
    r#type: MessageType,
    #[serde(skip_serializing_if = "Option::is_none")]
    headers: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    invocation_id: Option<String>,
    target: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arguments: Option<Vec<serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_ids: Option<Vec<String>>,
}

impl Invocation {
    pub fn create_single(target: impl Into<String>) -> Self {
        Invocation {
            r#type: MessageType::Invocation,
            headers: None,
            invocation_id: None,
            target: target.into(),
            arguments: Some(Vec::new()),
            stream_ids: None,
        }
    }

    pub fn create_multiple(target: impl Into<String>) -> Self {
        Invocation {
            r#type: MessageType::StreamInvocation,
            headers: None,
            invocation_id: None,
            target: target.into(),
            arguments: Some(Vec::new()),
            stream_ids: None,
        }
    }

    pub fn with_argument<T: Serialize>(&mut self, data: T) -> Result<(), String> {
        let json = MessageParser::to_json_value(&data)
            .map_err(|e| format!("Serialization error: {}", e))?;

        if let Some(ref mut vec) = self.arguments {
            vec.push(json);
        } else {
            self.arguments = Some(vec![json]);
        }

        Ok(())
    }

    pub fn with_invocation_id(&mut self, invocation_id: impl ToString) -> &mut Self {
        self.invocation_id = Some(invocation_id.to_string());
        self
    }

    #[allow(dead_code)]
    pub fn with_streams(&mut self, stream_ids: Vec<String>) -> &mut Self {
        if !stream_ids.is_empty() {
            self.stream_ids = Some(stream_ids);
        }
        self
    }

    pub(crate) fn get_invocation_id(&self) -> Option<String> {
        self.invocation_id.as_ref().map(|id| id.to_string())
    }

    pub(crate) fn get_target(&self) -> String {
        self.target.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_single_invocation() {
        let inv = Invocation::create_single("TestMethod");
        assert_eq!(inv.r#type, MessageType::Invocation);
        assert_eq!(inv.target, "TestMethod");
        assert!(inv.arguments.is_some());
        assert_eq!(inv.arguments.as_ref().unwrap().len(), 0);
        assert!(inv.invocation_id.is_none());
    }

    #[test]
    fn test_create_multiple_invocation() {
        let inv = Invocation::create_multiple("StreamMethod");
        assert_eq!(inv.r#type, MessageType::StreamInvocation);
        assert_eq!(inv.target, "StreamMethod");
        assert!(inv.arguments.is_some());
        assert!(inv.stream_ids.is_none());
    }

    #[test]
    fn test_with_argument() {
        let mut inv = Invocation::create_single("Test");
        let result = inv.with_argument("test string");
        assert!(result.is_ok());
        assert_eq!(inv.arguments.as_ref().unwrap().len(), 1);

        // Add another argument
        let result = inv.with_argument(42);
        assert!(result.is_ok());
        assert_eq!(inv.arguments.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn test_with_invocation_id() {
        let mut inv = Invocation::create_single("Test");
        inv.with_invocation_id("inv-123");
        assert_eq!(inv.invocation_id, Some("inv-123".to_string()));
        assert_eq!(inv.get_invocation_id(), Some("inv-123".to_string()));
    }

    #[test]
    fn test_with_streams() {
        let mut inv = Invocation::create_multiple("Test");
        inv.with_streams(vec!["stream1".to_string(), "stream2".to_string()]);
        assert!(inv.stream_ids.is_some());
        assert_eq!(inv.stream_ids.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn test_with_streams_empty() {
        let mut inv = Invocation::create_multiple("Test");
        inv.with_streams(vec![]);
        assert!(inv.stream_ids.is_none());
    }

    #[test]
    fn test_get_target() {
        let inv = Invocation::create_single("MyMethod");
        assert_eq!(inv.get_target(), "MyMethod");
    }

    #[test]
    fn test_invocation_serialization() {
        let mut inv = Invocation::create_single("TestMethod");
        inv.with_invocation_id("123");
        inv.with_argument("test").unwrap();
        inv.with_argument(42).unwrap();

        let json = serde_json::to_string(&inv).unwrap();
        assert!(json.contains("\"type\":1"));
        assert!(json.contains("\"target\":\"TestMethod\""));
        assert!(json.contains("\"invocationId\":\"123\""));
        assert!(json.contains("\"arguments\""));
    }

    #[test]
    fn test_completion_into_result_success() {
        let completion = Completion::create_result("123".to_string(), 42);
        assert_eq!(completion.into_result().unwrap(), 42);
    }

    #[test]
    fn test_completion_into_result_error() {
        let completion: Completion<i32> = Completion {
            r#type: MessageType::Completion,
            headers: None,
            invocation_id: "123".to_string(),
            result: None,
            error: Some("failed".to_string()),
        };

        assert_eq!(completion.into_result().unwrap_err(), "failed");
    }

    #[test]
    fn test_completion_into_result_empty_is_error() {
        let completion: Completion<i32> = Completion {
            r#type: MessageType::Completion,
            headers: None,
            invocation_id: "123".to_string(),
            result: None,
            error: None,
        };

        assert_eq!(
            completion.into_result().unwrap_err(),
            "Completion did not include a result or error"
        );
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Completion<R> {
    r#type: MessageType,
    #[serde(skip_serializing_if = "Option::is_none")]
    headers: Option<HashMap<String, String>>,
    invocation_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<R>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl<R> Completion<R> {
    pub fn create_result(invocation_id: String, data: R) -> Self {
        Completion {
            r#type: MessageType::Completion,
            invocation_id,
            result: Some(data),
            error: None,
            headers: None,
        }
    }

    #[allow(dead_code)]
    pub fn is_error(&self) -> bool {
        self.error.is_some()
    }

    pub fn into_result(self) -> Result<R, String> {
        match (self.result, self.error) {
            (Some(result), _) => Ok(result),
            (None, Some(error)) => Err(error),
            (None, None) => Err("Completion did not include a result or error".to_string()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct CancelInvocation {
    r#type: MessageType,
    #[serde(skip_serializing_if = "Option::is_none")]
    headers: Option<HashMap<String, String>>,
    pub invocation_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PossibleInvocation {
    r#type: MessageType,
    #[serde(skip_serializing_if = "Option::is_none")]
    headers: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub invocation_id: Option<String>,
    pub target: Option<String>,
}
