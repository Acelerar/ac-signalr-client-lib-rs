use super::Storage;
use super::UpdatableAction;
use crate::client::Protocol;
use crate::protocol::negotiate::MessageType;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use tracing::error;
use tracing::info;

type ActionMap = Arc<Mutex<HashMap<String, Mutex<Box<dyn UpdatableAction>>>>>;
type DeferredMessages = Arc<Mutex<HashMap<String, VecDeque<(Vec<u8>, MessageType, Protocol)>>>>;

#[allow(dead_code)]
const DEFAULT_MAX_DEFERRED_MESSAGES_PER_TARGET: usize = 4096;

#[derive(Clone)]
pub struct UpdatableActionStorage {
    data: ActionMap,
    deferred_messages: DeferredMessages,
    deferred_message_capacity: usize,
    index: Arc<Mutex<usize>>,
}

impl UpdatableActionStorage {
    #[allow(clippy::arc_with_non_send_sync)]
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self::new_with_deferred_message_capacity(DEFAULT_MAX_DEFERRED_MESSAGES_PER_TARGET)
    }

    #[allow(clippy::arc_with_non_send_sync)]
    pub(crate) fn new_with_deferred_message_capacity(capacity: usize) -> Self {
        UpdatableActionStorage {
            data: Arc::new(Mutex::new(HashMap::new())),
            deferred_messages: Arc::new(Mutex::new(HashMap::new())),
            deferred_message_capacity: capacity,
            index: Arc::new(Mutex::new(0)),
        }
    }
}

impl Drop for UpdatableActionStorage {
    fn drop(&mut self) {
        self.dispose();
    }
}

unsafe impl Send for UpdatableActionStorage {}

impl Storage for UpdatableActionStorage {
    fn insert(&mut self, key: String, action: impl UpdatableAction + 'static) {
        if let Ok(mut data) = self.data.lock() {
            use std::collections::hash_map::Entry;
            match data.entry(key.clone()) {
                Entry::Vacant(e) => {
                    e.insert(Mutex::new(Box::new(action)));
                }
                Entry::Occupied(_) => {
                    error!("Key {} is already registered as an action", key);
                }
            }
        } else {
            error!("Cannot lock storage");
        }
    }

    fn contains(&self, key: String) -> bool {
        if let Ok(data) = self.data.lock() {
            data.contains_key(&key)
        } else {
            error!("Cannot lock storage");
            false
        }
    }

    fn update(
        &mut self,
        key: String,
        mut f: impl FnMut(&mut Box<dyn UpdatableAction>) -> Result<(), String>,
    ) -> Result<(), String> {
        if let Ok(mut data) = self.data.lock() {
            if let Some(action) = data.get_mut(&key) {
                if let Ok(a) = action.get_mut() {
                    f(a)
                } else {
                    Err("Cannot unlock action".to_string())
                }
            } else {
                Err(format!("Key {} is not found in registered actions", key))
            }
        } else {
            Err("Cannot lock storage".to_string())
        }
    }

    fn remove(&mut self, key: String) {
        if let Ok(mut data) = self.data.lock() {
            if let Some(ret) = data.remove(&key) {
                if let Ok(r) = ret.into_inner() {
                    drop(r);
                }
            }
        } else {
            error!("Cannot lock storage");
        }
    }

    fn defer_message(
        &mut self,
        key: String,
        message: Vec<u8>,
        message_type: MessageType,
        protocol: Protocol,
    ) {
        let Ok(mut data) = self.data.lock() else {
            error!("Cannot lock storage");
            return;
        };

        if let Some(action_result) = data.get_mut(&key).map(|action| match action.get_mut() {
            Ok(action) => action.update_with(&message, message_type, protocol),
            Err(_) => Err("Cannot unlock action".to_string()),
        }) {
            match action_result {
                Ok(()) => return,
                Err(error) => {
                    error!("Failed to dispatch deferred callback invocation: {}", error);
                    return;
                }
            }
        }

        let Ok(mut deferred_messages) = self.deferred_messages.lock() else {
            error!("Cannot lock deferred callback storage");
            return;
        };

        let messages = deferred_messages.entry(key).or_default();
        messages.push_back((message, message_type, protocol));
        if messages.len() > self.deferred_message_capacity {
            messages.pop_front();
        }
    }

    fn replay_deferred(&mut self, key: String) {
        let messages = match self.deferred_messages.lock() {
            Ok(mut deferred_messages) => deferred_messages.remove(&key),
            Err(_) => {
                error!("Cannot lock deferred callback storage");
                None
            }
        };

        let Some(messages) = messages else {
            return;
        };

        for (message, message_type, protocol) in messages {
            if let Err(error) = self.update(key.clone(), |action| {
                action.update_with(&message, message_type, protocol)
            }) {
                error!("Failed to replay deferred callback invocation: {}", error);
            }
        }
    }

    fn dispose(&mut self) {
        let count = Arc::strong_count(&self.data);

        if count == 1 {
            info!("Clearing storage...");
            if let Ok(mut data) = self.data.lock() {
                data.clear();
            } else {
                error!("Cannot lock storage");
            }
        }
    }

    fn increment(&mut self) -> usize {
        let mut index = match self.index.lock() {
            Ok(index) => index,
            Err(poisoned) => {
                error!("Storage index mutex poisoned");
                poisoned.into_inner()
            }
        };

        *index += 1;

        *index
    }

    fn cancel_pending(&mut self, reason: &str) {
        if let Ok(mut data) = self.data.lock() {
            let mut completed_keys = Vec::new();

            for (key, action) in data.iter_mut() {
                if let Ok(action) = action.get_mut() {
                    action.cancel(reason);
                    if action.is_completed() {
                        completed_keys.push(key.clone());
                    }
                } else {
                    error!("Cannot unlock action {}", key);
                }
            }

            for key in completed_keys {
                data.remove(&key);
            }
        } else {
            error!("Cannot lock storage");
        }
    }
}
