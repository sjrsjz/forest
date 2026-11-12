use crate::{DispatchResult, Token, host, operation};

/// A task-owned lock. Moving it to a different root guest task does not transfer
/// ownership; the host reclaims any remaining permits after the owner finishes.
#[must_use = "dropping the guard releases the lock"]
pub struct LockGuard {
    key: Option<Token>,
    task: u64,
}

impl LockGuard {
    pub(crate) fn new(key: Token, task: u64) -> Self {
        Self {
            key: Some(key),
            task,
        }
    }

    pub fn unlock(mut self) -> DispatchResult<()> {
        host::unlock(self.key.take().expect("live lock guard"))
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        // During lifecycle retirement there is no ordinary task poll. The host
        // owns cleanup and releases permits only after native operations end.
        if operation::current() == Some(self.task)
            && let Some(key) = self.key.take()
        {
            let _ = host::unlock(key);
        }
    }
}
