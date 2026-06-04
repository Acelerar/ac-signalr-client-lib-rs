use core::future::Future;

use self::messages::MessageParser;
use super::SignalRClient;
use crate::protocol::invoke::Completion;
use crate::protocol::invoke::Invocation;
use crate::protocol::messages;
use serde::de::DeserializeOwned;
use serde::Serialize;

/// Context passed to a callback registered with [`crate::SignalRClient::register`].
pub struct InvocationContext {
    /// A cloned client handle that can be used to send additional hub messages.
    pub client: SignalRClient,
    invocation: Invocation,
}

impl InvocationContext {
    pub(crate) fn create(client: SignalRClient, invocation: Invocation) -> Self {
        InvocationContext { client, invocation }
    }

    /// Deserializes the invocation argument at `index` into `T`.
    pub fn argument<T: DeserializeOwned + Unpin>(&self, index: usize) -> Result<T, String> {
        let arguments = self
            .invocation
            .arguments
            .as_ref()
            .ok_or("There are no arguments for the invocation")?;

        let arg = arguments.get(index).ok_or(format!(
            "The argument does not exist at the given index {}",
            index
        ))?;

        let strvalue = arg.to_string();
        MessageParser::parse_message::<T>(&strvalue).map_err(|_| {
            format!(
                "The argument cannot be deserialized to the requested type {:?}",
                arg.as_str()
            )
        })
    }

    /// Sends a completion message back to the hub for the active invocation.
    ///
    /// This only succeeds when the incoming invocation carried an invocation id.
    pub async fn complete<T: Serialize>(&mut self, result: T) -> Result<(), String> {
        let invocation_id = self.invocation.get_invocation_id().ok_or(
            "The completion cannot be sent, because there was no invocation id for the call",
        )?;

        let completion = Completion::create_result(invocation_id, result);
        self.client.send_direct(completion).await
    }

    /// Spawns follow-up async work on the Tokio runtime.
    #[cfg(not(target_family = "wasm"))]
    pub fn spawn<F>(future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(future);
    }
}
