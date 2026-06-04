use super::negotiate::MessageType;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StreamItem<I> {
    r#type: MessageType,
    #[serde(skip_serializing_if = "Option::is_none")]
    headers: Option<HashMap<String, String>>,
    pub(crate) invocation_id: String,
    pub(crate) item: I,
}
