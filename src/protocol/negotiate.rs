use serde::Deserialize;
use serde::Serialize;
use serde_repr::Deserialize_repr;
use serde_repr::Serialize_repr;

#[derive(Debug, Serialize_repr, Deserialize_repr, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    Invocation = 1,
    StreamItem = 2,
    Completion = 3,
    StreamInvocation = 4,
    CancelInvocation = 5,
    Ping = 6,
    Close = 7,
    Other = 8,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NegotiateResponseV0 {
    pub connection_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_token: Option<String>,
    pub negotiate_version: u8,
    pub available_transports: Vec<TransportSpec>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransportSpec {
    pub transport: String,
    pub transfer_formats: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HandshakeRequest {
    protocol: String,
    version: u8,
}

impl HandshakeRequest {
    pub fn new(protocol: impl Into<String>) -> Self {
        HandshakeRequest {
            protocol: protocol.into(),
            version: 1,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct HandshakeResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Ping {
    r#type: MessageType,
}

impl Ping {
    pub fn new() -> Self {
        Ping {
            r#type: MessageType::Ping,
        }
    }

    pub fn message_type(&self) -> MessageType {
        self.r#type
    }
}

impl Default for Ping {
    fn default() -> Self {
        Ping::new()
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct Close {
    r#type: MessageType,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    allow_reconnect: Option<bool>,
}
