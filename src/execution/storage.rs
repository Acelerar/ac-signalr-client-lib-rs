use super::callback::CallbackAction;
use super::enumerable::EnumerableAction;
use super::invocation::InvocationAction;
use super::UpdatableAction;
use crate::client::Protocol;
use crate::client::SignalRClient;
use crate::completer::CompletedFuture;
use crate::completer::ManualFuture;
use crate::completer::ManualFutureCompleter;
use crate::completer::ManualStream;
use crate::protocol::invoke::Invocation;
use crate::protocol::invoke::PossibleInvocation;
use crate::protocol::messages::MessageParser;
use crate::protocol::negotiate;
use crate::protocol::negotiate::MessageType;
use crate::InvocationContext;
use serde::de::DeserializeOwned;
use tracing::debug;
use tracing::info;

#[allow(dead_code)]
#[derive(Clone)]
pub struct ManualFutureState {
    completer: Option<ManualFutureCompleter<bool>>,
    future: Option<ManualFuture<bool>>,
}

impl ManualFutureState {
    #[allow(dead_code)]
    pub fn new() -> Self {
        let (f, c) = ManualFuture::new();

        ManualFutureState {
            completer: Some(c),
            future: Some(f),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn complete(&mut self, value: bool) {
        if let Some(completer) = self.completer.take() {
            completer.complete(value);
        }
    }

    #[allow(dead_code)]
    pub(crate) async fn awaiter(&mut self) -> bool {
        if let Some(future) = self.future.take() {
            future.await
        } else {
            CompletedFuture::new(false).await
        }
    }
}

pub trait Storage: Clone {
    fn insert(&mut self, key: String, action: impl UpdatableAction + 'static);
    #[allow(dead_code)]
    fn contains(&self, key: String) -> bool;
    fn update(
        &mut self,
        key: String,
        f: impl FnMut(&mut Box<dyn UpdatableAction>) -> Result<(), String>,
    ) -> Result<(), String>;
    fn remove(&mut self, key: String);
    fn dispose(&mut self);
    fn increment(&mut self) -> usize;
    fn cancel_pending(&mut self, reason: &str);

    /// Retain an invocation that arrived before its callback was registered.
    ///
    /// Implementations can use this to bridge the small gap between a
    /// connection becoming active and the caller installing its callbacks.
    fn defer_message(
        &mut self,
        _key: String,
        _message: Vec<u8>,
        _message_type: MessageType,
        _protocol: Protocol,
    ) {
    }

    /// Replay invocations retained for a callback after registering it.
    fn replay_deferred(&mut self, _key: String) {}

    fn create_key(&mut self, target: String) -> String {
        let index = self.increment();

        format!("{}_{}", target, index)
    }

    fn add_callback(
        &mut self,
        target: String,
        callback: impl Fn(InvocationContext) + 'static,
        client: SignalRClient,
    ) {
        debug!("Adding a callback for key {}", target);
        self.insert(
            target.clone(),
            CallbackAction::create(target.clone(), callback, client),
        );
        self.replay_deferred(target);
    }

    fn add_invocation<R: 'static + DeserializeOwned + Unpin>(
        &mut self,
        invocation_id: String,
    ) -> ManualFuture<Result<R, String>> {
        let (invocation, f) = InvocationAction::<R>::new(invocation_id.clone());

        debug!("Inserting invocation for key {}", invocation_id);
        self.insert(invocation_id, invocation);

        f
    }

    fn add_stream<R: 'static + DeserializeOwned + Unpin>(
        &mut self,
        invocation_id: String,
    ) -> ManualStream<R> {
        let (stream, f) = EnumerableAction::<R>::new(invocation_id.clone());

        self.insert(invocation_id, stream);

        f
    }

    fn process_message(
        &mut self,
        message: &[u8],
        message_type: MessageType,
        protocol: Protocol,
    ) -> Result<(), String> {
        debug!(
            "MESSAGE: {:?} -> {:?}",
            message_type,
            String::from_utf8_lossy(message)
        );

        match message_type {
            negotiate::MessageType::Invocation => {
                debug!(
                    "Server invocation {:?} -> {:?}",
                    message_type,
                    String::from_utf8_lossy(message)
                );
                let invocation = MessageParser::deserialize::<Invocation>(message, protocol)?;
                let target = invocation.get_target();

                if self.contains(target.clone()) {
                    self.update(target, |i| i.update_with(message, message_type, protocol))?;
                } else {
                    self.defer_message(target, message.to_vec(), message_type, protocol);
                }
            }
            negotiate::MessageType::StreamItem => {
                let invocation =
                    MessageParser::deserialize::<PossibleInvocation>(message, protocol)?;

                if let Some(invocation_id) = invocation.invocation_id {
                    let key = invocation_id.clone();
                    self.update(key, |i| i.update_with(message, message_type, protocol))?;
                }
            }
            negotiate::MessageType::Completion => {
                let invocation =
                    MessageParser::deserialize::<PossibleInvocation>(message, protocol)?;

                info!("Completion received {:?}", String::from_utf8_lossy(message));

                if let Some(invocation_id) = invocation.invocation_id {
                    let key = invocation_id.clone();
                    self.update(key.clone(), |i| {
                        i.update_with(message, message_type, protocol)
                    })?;

                    self.remove(key);
                }
            }
            negotiate::MessageType::StreamInvocation => {
                debug!("Stream invocation is arrived");
            }
            negotiate::MessageType::CancelInvocation => {
                debug!("Cancel invocation is arrived");
            }
            negotiate::MessageType::Ping => {
                debug!("Ping is arrived");
            }
            negotiate::MessageType::Close => {
                debug!("Close is arrived");
            }
            negotiate::MessageType::Other => {
                debug!("Other is arrived");
            }
        }

        Ok(())
    }
}

/// Handle returned by [`crate::SignalRClient::register`] for unregistering callbacks.
pub trait CallbackHandler {
    /// Removes the registered callback from the client.
    fn unregister(self);
}

pub(crate) struct StorageUnregistrationHandler<T>
where
    T: Storage,
{
    storage: T,
    key: String,
}

impl<T: Storage> StorageUnregistrationHandler<T> {
    pub(crate) fn new(storage: T, key: String) -> Self {
        StorageUnregistrationHandler { storage, key }
    }
}

impl<T: Storage> CallbackHandler for StorageUnregistrationHandler<T> {
    fn unregister(mut self) {
        self.storage.remove(self.key);
    }
}
