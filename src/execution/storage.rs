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
    _completer: Option<ManualFutureCompleter<bool>>,
    _future: Option<ManualFuture<bool>>,
}

impl ManualFutureState {
    #[allow(dead_code)]
    pub fn new() -> Self {
        let (f, c) = ManualFuture::new();

        ManualFutureState {
            _completer: Some(c),
            _future: Some(f),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn complete(&mut self, value: bool) {
        if self._completer.is_some() {
            let completer = self._completer.take().unwrap();

            completer.complete(value);
        }
    }

    #[allow(dead_code)]
    pub(crate) async fn awaiter(&mut self) -> bool {
        if self._future.is_some() {
            self._future.take().unwrap().await
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

                self.update(invocation.get_target(), |i| {
                    i.update_with(message, message_type, protocol)
                })?;
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
    _storage: T,
    _key: String,
}

impl<T: Storage> StorageUnregistrationHandler<T> {
    pub(crate) fn new(storage: T, key: String) -> Self {
        StorageUnregistrationHandler {
            _key: key,
            _storage: storage,
        }
    }
}

impl<T: Storage> CallbackHandler for StorageUnregistrationHandler<T> {
    fn unregister(mut self) {
        self._storage.remove(self._key);
    }
}
