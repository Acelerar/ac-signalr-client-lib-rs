use super::actions::UpdatableAction;
use crate::client::Protocol;
use crate::client::SignalRClient;
use crate::protocol::invoke::Invocation;
use crate::protocol::messages::MessageParser;
use crate::protocol::negotiate::MessageType;
use crate::InvocationContext;

pub(crate) struct CallbackAction {
    #[allow(dead_code)]
    target: String,
    callback: Box<dyn Fn(InvocationContext) + 'static>,
    client: SignalRClient,
}

impl CallbackAction {
    pub(crate) fn create(
        target: String,
        callback: impl Fn(InvocationContext) + 'static,
        client: SignalRClient,
    ) -> CallbackAction {
        CallbackAction {
            target,
            callback: Box::new(callback),
            client,
        }
    }
}

impl UpdatableAction for CallbackAction {
    fn update_with(
        &mut self,
        message: &[u8],
        message_type: MessageType,
        protocol: Protocol,
    ) -> Result<(), String> {
        match message_type {
            MessageType::Invocation => {
                let invocation: Invocation = MessageParser::deserialize(message, protocol)?;
                let context = InvocationContext::create(self.client.clone(), invocation);

                (self.callback)(context);
                Ok(())
            }
            _ => Err("Callbacks accept only invocation data".to_string()),
        }
    }

    fn cancel(&mut self, _reason: &str) {}

    fn is_completed(&self) -> bool {
        false
    }

    fn dispose(self) {
        drop(self.callback);
        drop(self.client);
        drop(self.target);
    }
}
