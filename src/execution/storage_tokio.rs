use super::Storage;
use super::UpdatableAction;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use tracing::error;
use tracing::info;

type ActionMap = Arc<Mutex<HashMap<String, Mutex<Box<dyn UpdatableAction>>>>>;

#[derive(Clone)]
pub struct UpdatableActionStorage {
    _data: ActionMap,
    _index: Arc<Mutex<usize>>,
}

impl UpdatableActionStorage {
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn new() -> Self {
        UpdatableActionStorage {
            _data: Arc::new(Mutex::new(HashMap::new())),
            _index: Arc::new(Mutex::new(0)),
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
        if let Ok(mut data) = self._data.lock() {
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
        if let Ok(data) = self._data.lock() {
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
        if let Ok(mut data) = self._data.lock() {
            if let Some(action) = data.get_mut(&key) {
                if let Ok(a) = action.get_mut() {
                    (f)(a)
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
        if let Ok(mut data) = self._data.lock() {
            if let Some(ret) = data.remove(&key) {
                if let Ok(r) = ret.into_inner() {
                    drop(r);
                }
            }
        } else {
            error!("Cannot lock storage");
        }
    }

    fn dispose(&mut self) {
        let count = Arc::strong_count(&self._data);

        if count == 1 {
            info!("Clearing storage...");
            if let Ok(mut data) = self._data.lock() {
                data.clear();
            } else {
                error!("Cannot lock storage");
            }
        }
    }

    fn increment(&mut self) -> usize {
        let mut index = self._index.lock().unwrap();

        *index += 1;

        *index
    }

    fn cancel_pending(&mut self, reason: &str) {
        if let Ok(mut data) = self._data.lock() {
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
