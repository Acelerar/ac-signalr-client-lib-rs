use futures::Stream;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tracing::info;

use crate::communication::Communication;
use crate::communication::CommunicationClient;
use crate::communication::HttpClient;
use crate::execution::ArgumentConfiguration;
use crate::execution::CallbackHandler;
use crate::execution::Storage;
use crate::execution::StorageUnregistrationHandler;
use crate::execution::UpdatableActionStorage;
use crate::protocol::invoke::Invocation;

use super::ConnectionConfiguration;
use super::InvocationContext;

/// Client for connecting to a SignalR hub from native Rust code.
///
/// Cloned instances share the same underlying connection.
pub struct SignalRClient {
    _actions: UpdatableActionStorage,
    _connection: CommunicationClient,
}

impl Drop for SignalRClient {
    fn drop(&mut self) {
        self._connection.disconnect();
    }
}

impl SignalRClient {
    /// Connects to `hub` on `domain` using the default configuration.
    ///
    /// Defaults are secure transport, JSON serialization, and reconnect disabled.
    pub async fn connect(domain: &str, hub: &str) -> Result<Self, String> {
        SignalRClient::connect_internal(domain, hub, None::<fn(&mut ConnectionConfiguration)>).await
    }

    /// Connects to `hub` on `domain` and lets the caller customize the connection.
    ///
    /// The `options` closure receives a mutable [`ConnectionConfiguration`] before
    /// negotiation starts.
    pub async fn connect_with<F>(domain: &str, hub: &str, options: F) -> Result<Self, String>
    where
        F: FnMut(&mut ConnectionConfiguration),
    {
        SignalRClient::connect_internal(domain, hub, Some(options)).await
    }

    async fn connect_internal<F>(
        domain: &str,
        hub: &str,
        options: Option<F>,
    ) -> Result<Self, String>
    where
        F: FnMut(&mut ConnectionConfiguration),
    {
        let mut config = ConnectionConfiguration::new(domain.to_string(), hub.to_string());

        if let Some(mut ops) = options {
            (ops)(&mut config);
        }

        let configuration = HttpClient::negotiate(config).await?;
        info!("Negotiation successfull: {:?}", configuration);

        let client = CommunicationClient::connect(&configuration).await?;
        let storage = client.get_storage()?;

        Ok(SignalRClient {
            _actions: storage,
            _connection: client,
        })
    }

    /// Registers a callback for a hub-to-client invocation target.
    ///
    /// The returned handler can be used later to unregister the callback. Keep
    /// callback work short and offload async work with [`InvocationContext::spawn`].
    pub fn register(
        &mut self,
        target: String,
        callback: impl Fn(InvocationContext) + 'static,
    ) -> impl CallbackHandler {
        self._actions
            .add_callback(target.clone(), callback, self.clone());

        StorageUnregistrationHandler::new(self._actions.clone(), target.clone())
    }

    /// Invokes a hub method and waits for a single completion payload.
    pub async fn invoke<T: 'static + DeserializeOwned + Unpin>(
        &mut self,
        target: String,
    ) -> Result<T, String> {
        return self
            .invoke_internal(target, None::<fn(&mut ArgumentConfiguration)>)
            .await;
    }

    /// Invokes a hub method with serialized arguments and waits for a completion payload.
    pub async fn invoke_with_args<T: 'static + DeserializeOwned + Unpin, F>(
        &mut self,
        target: String,
        configuration: F,
    ) -> Result<T, String>
    where
        F: FnMut(&mut ArgumentConfiguration),
    {
        return self.invoke_internal(target, Some(configuration)).await;
    }

    async fn invoke_internal<T: 'static + DeserializeOwned + Unpin, F>(
        &mut self,
        target: String,
        configuration: Option<F>,
    ) -> Result<T, String>
    where
        F: FnMut(&mut ArgumentConfiguration),
    {
        let invocation_id = self._actions.create_key(target.clone());
        let ret = self._actions.add_invocation::<T>(invocation_id.clone());

        let mut invocation = Invocation::create_single(target.clone());
        invocation.with_invocation_id(&invocation_id);

        if let Some(mut config) = configuration {
            let mut args = ArgumentConfiguration::new(invocation);
            config(&mut args);

            invocation = args.build_invocation();
        }

        if let Err(error) = self._connection.send(&invocation).await {
            self._actions.remove(invocation_id);
            return Err(error);
        }

        ret.await
    }

    /// Sends a fire-and-forget hub invocation without waiting for a completion.
    pub async fn send(&mut self, target: String) -> Result<(), String> {
        return self
            .send_internal(target, None::<fn(&mut ArgumentConfiguration)>)
            .await;
    }

    /// Sends a fire-and-forget hub invocation with serialized arguments.
    pub async fn send_with_args<F>(
        &mut self,
        target: String,
        configuration: F,
    ) -> Result<(), String>
    where
        F: FnMut(&mut ArgumentConfiguration),
    {
        return self.send_internal(target, Some(configuration)).await;
    }

    async fn send_internal<F>(
        &mut self,
        target: String,
        configuration: Option<F>,
    ) -> Result<(), String>
    where
        F: FnMut(&mut ArgumentConfiguration),
    {
        let mut invocation = Invocation::create_single(target.clone());

        if let Some(mut config) = configuration {
            let mut args = ArgumentConfiguration::new(invocation);
            config(&mut args);

            invocation = args.build_invocation();
        }

        self._connection.send(&invocation).await
    }

    pub(crate) async fn send_direct<T: Serialize>(&mut self, data: T) -> Result<(), String> {
        self._connection.send(&data).await
    }

    /// Starts a server-streaming invocation and returns the resulting item stream.
    pub async fn enumerate<T: 'static + DeserializeOwned + Unpin>(
        &mut self,
        target: String,
    ) -> impl Stream<Item = T> {
        return self
            .enumerate_internal(target, None::<fn(&mut ArgumentConfiguration)>)
            .await;
    }

    /// Starts a server-streaming invocation with serialized arguments.
    pub async fn enumerate_with_args<T: 'static + DeserializeOwned + Unpin, F>(
        &mut self,
        target: String,
        configuration: F,
    ) -> impl Stream<Item = T>
    where
        F: FnMut(&mut ArgumentConfiguration),
    {
        return self.enumerate_internal(target, Some(configuration)).await;
    }

    async fn enumerate_internal<T: 'static + DeserializeOwned + Unpin, F>(
        &mut self,
        target: String,
        configuration: Option<F>,
    ) -> impl Stream<Item = T>
    where
        F: FnMut(&mut ArgumentConfiguration),
    {
        let invocation_id = self._actions.create_key(target.clone());
        let res = self._actions.add_stream::<T>(invocation_id.clone());
        let mut invocation = Invocation::create_multiple(target.clone());
        invocation.with_invocation_id(&invocation_id);

        if let Some(mut config) = configuration {
            let mut args = ArgumentConfiguration::new(invocation);
            config(&mut args);

            invocation = args.build_invocation();
        }

        if self._connection.send(&invocation).await.is_err() {
            self._actions.remove(invocation_id);
        }

        res
    }

    /// Disconnects this client handle.
    ///
    /// The underlying connection closes when this is the last live clone.
    pub fn disconnect(mut self) {
        self._connection.disconnect();
    }

    /// Gracefully disconnects the underlying connection for all clones.
    ///
    /// This sends a WebSocket close frame before tearing down the shared
    /// connection state, which is useful for application shutdown paths that
    /// should close cleanly on the server side.
    pub async fn disconnect_gracefully(mut self) -> Result<(), String> {
        self._connection.disconnect_gracefully().await
    }
}

impl Clone for SignalRClient {
    fn clone(&self) -> Self {
        Self {
            _actions: self._actions.clone(),
            _connection: self._connection.clone(),
        }
    }
}
