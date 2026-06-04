use crate::client::Protocol;
use crate::protocol::negotiate::MessageType;

pub(crate) trait UpdatableAction {
    fn update_with(
        &mut self,
        message: &[u8],
        message_type: MessageType,
        protocol: Protocol,
    ) -> Result<(), String>;
    fn cancel(&mut self, reason: &str);
    #[allow(dead_code)]
    fn is_completed(&self) -> bool;
    #[allow(dead_code)]
    fn dispose(self);
}
