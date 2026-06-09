use crate::client::Protocol;
use crate::completer::ManualFuture;
use crate::completer::ManualFutureCompleter;
use crate::protocol::invoke::Completion;
use crate::protocol::negotiate::MessageType;
use serde::de::DeserializeOwned;
use tracing::error;
use tracing::info;

use crate::protocol::messages::MessageParser;

use super::actions::UpdatableAction;

pub(crate) struct InvocationAction<R: DeserializeOwned + Unpin> {
    invocation_id: String,
    completer: Option<ManualFutureCompleter<Result<R, String>>>,
}

impl<R: DeserializeOwned + Unpin> InvocationAction<R> {
    pub fn new(invocation_id: String) -> (Self, ManualFuture<Result<R, String>>) {
        let (f, c) = ManualFuture::new();
        let invocation = InvocationAction {
            invocation_id,
            completer: Some(c),
        };

        (invocation, f)
    }

    #[allow(dead_code)]
    pub fn completable(&self) -> bool {
        self.completer.is_some()
    }

    pub fn complete(&mut self, result: Result<R, String>) {
        info!("Trying to get future completer from Invocation Action");
        if let Some(completer) = self.completer.take() {
            info!("Future completer is taken");
            completer.complete(result);
            info!("Future completer is completed");
        } else {
            error!("Invocation {} is already completed", self.invocation_id);
        }
    }

    fn dispose_internal(&mut self) {
        if let Some(c) = self.completer.take() {
            c.complete(Err("Invocation was cancelled".to_string()));
        }
    }
}

impl<R: DeserializeOwned + Unpin> Drop for InvocationAction<R> {
    fn drop(&mut self) {
        self.dispose_internal();
    }
}

impl<R: DeserializeOwned + Unpin> UpdatableAction for InvocationAction<R> {
    fn update_with(
        &mut self,
        message: &[u8],
        message_type: MessageType,
        protocol: Protocol,
    ) -> Result<(), String> {
        match message_type {
            MessageType::Completion => {
                let completion = MessageParser::deserialize::<Completion<R>>(message, protocol)?;
                info!("Completion is parsed");
                self.complete(completion.into_result());
                Ok(())
            }
            MessageType::Invocation
            | MessageType::StreamItem
            | MessageType::StreamInvocation
            | MessageType::CancelInvocation
            | MessageType::Ping
            | MessageType::Close
            | MessageType::Other => Err(format!(
                "Cannot complete invocation {}, with message {:?}",
                self.invocation_id,
                String::from_utf8_lossy(message)
            )),
        }
    }

    fn cancel(&mut self, reason: &str) {
        self.complete(Err(reason.to_string()));
    }

    fn is_completed(&self) -> bool {
        self.completer.is_none()
    }

    fn dispose(mut self) {
        self.dispose_internal();
    }
}
