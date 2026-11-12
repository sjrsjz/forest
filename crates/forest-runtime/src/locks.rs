//! Instance-local asynchronous locks. A permit is owned by a pending operation until
//! delivery, then by its task. Dropping either owner releases the same permit.
use forest_protocol::Token;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

#[derive(Default)]
pub(crate) struct Locks {
    entries: Mutex<HashMap<Token, Weak<AsyncMutex<()>>>>,
}

pub(crate) struct Permit {
    pub key: Token,
    _guard: OwnedMutexGuard<()>,
}

impl Locks {
    pub async fn acquire(&self, key: Token) -> Permit {
        let lock = {
            let mut entries = self.entries.lock().unwrap();
            entries.retain(|_, entry| entry.strong_count() > 0);
            match entries.get(&key).and_then(Weak::upgrade) {
                Some(lock) => lock,
                None => {
                    let lock = Arc::new(AsyncMutex::new(()));
                    entries.insert(key.clone(), Arc::downgrade(&lock));
                    lock
                }
            }
        };
        Permit {
            key,
            _guard: lock.lock_owned().await,
        }
    }
}
