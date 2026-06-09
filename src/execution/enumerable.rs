use serde::de::DeserializeOwned;
use tracing::error;

use crate::client::Protocol;
use crate::completer::ManualStream;
use crate::completer::ManualStreamCompleter;
use crate::protocol::invoke::Completion;
use crate::protocol::messages::MessageParser;
use crate::protocol::negotiate::MessageType;
use crate::protocol::streaming::StreamItem;

use super::actions::UpdatableAction;

pub(crate) struct EnumerableAction<R: DeserializeOwned + Unpin> {
    invocation_id: String,
    completer: ManualStreamCompleter<R>,
    completed: bool,
}

impl<R: DeserializeOwned + Unpin> EnumerableAction<R> {
    pub fn new(invocation_id: String) -> (Self, ManualStream<R>) {
        let (s, c) = ManualStream::create();

        (
            EnumerableAction {
                invocation_id,
                completer: c,
                completed: false,
            },
            s,
        )
    }

    fn dispose_internal(&mut self) {
        self.completed = true;
        self.completer.close();
    }
}

impl<R: DeserializeOwned + Unpin> Drop for EnumerableAction<R> {
    fn drop(&mut self) {
        self.dispose_internal();
    }
}

impl<R: DeserializeOwned + Unpin> UpdatableAction for EnumerableAction<R> {
    fn update_with(
        &mut self,
        message: &[u8],
        message_type: MessageType,
        protocol: Protocol,
    ) -> Result<(), String> {
        match message_type {
            MessageType::StreamItem => {
                let item = MessageParser::deserialize::<StreamItem<R>>(message, protocol)?;
                self.completer.push(item.item);
                Ok(())
            }
            MessageType::Completion => {
                let completion = MessageParser::deserialize::<Completion<R>>(message, protocol)?;
                if let Err(error) = completion.into_result() {
                    error!(
                        "Stream {} completed with error: {}",
                        self.invocation_id, error
                    );
                }
                self.completer.close();
                Ok(())
            }
            MessageType::Invocation
            | MessageType::StreamInvocation
            | MessageType::CancelInvocation
            | MessageType::Ping
            | MessageType::Close
            | MessageType::Other => Err(format!(
                "Cannot update stream {} with message {:?}",
                self.invocation_id,
                String::from_utf8_lossy(message)
            )),
        }
    }

    fn cancel(&mut self, _reason: &str) {
        self.dispose_internal();
    }

    fn is_completed(&self) -> bool {
        self.completed
    }

    fn dispose(mut self) {
        self.dispose_internal();
    }
}
