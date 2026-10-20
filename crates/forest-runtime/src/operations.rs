use crate::ready::TaskSignal;
use forest_protocol::{DispatchError, abi};
use serde::Serialize;
use std::{
    collections::HashMap,
    future::Future,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::task::{AbortHandle, JoinSet};

enum CompletionState {
    Pending,
    Ready(Vec<u8>),
    Closed,
}
struct Completion {
    state: Mutex<CompletionState>,
    signal: Weak<TaskSignal>,
}
struct Operation {
    task: u64,
    completion: Arc<Completion>,
    abort: AbortHandle,
    cancellable: bool,
}

#[derive(Default)]
pub(crate) struct Operations {
    next: AtomicU64,
    entries: Mutex<HashMap<u64, Operation>>,
    jobs: Mutex<HashMap<u64, JoinSet<()>>>,
}

impl Operations {
    pub fn start<T: Serialize + Send + 'static>(
        &self,
        signal: &Arc<TaskSignal>,
        cancellable: bool,
        future: impl Future<Output = T> + Send + 'static,
    ) -> u64 {
        let id = self
            .next
            .fetch_add(1, Ordering::Relaxed)
            .checked_add(1)
            .expect("operation id exhaustion");
        let completion = Arc::new(Completion {
            state: Mutex::new(CompletionState::Pending),
            signal: Arc::downgrade(signal),
        });
        let result = completion.clone();
        let mut groups = self.jobs.lock().unwrap();
        let jobs = groups.entry(signal.id).or_default();
        while let Some(joined) = jobs.try_join_next() {
            if let Err(error) = joined {
                assert!(error.is_cancelled(), "host operation panicked: {error}");
            }
        }
        let abort = jobs.spawn(async move {
            let output = future.await;
            let bytes = abi::encode(&output).unwrap_or_else(|_| {
                abi::encode(&Err::<(), _>(DispatchError::OutputContract)).unwrap()
            });
            let mut state = result.state.lock().unwrap();
            if matches!(*state, CompletionState::Closed) {
                return;
            }
            assert!(matches!(*state, CompletionState::Pending));
            *state = CompletionState::Ready(bytes);
            drop(state);
            if let Some(signal) = result.signal.upgrade() {
                signal.notify();
            }
        });
        self.entries.lock().unwrap().insert(
            id,
            Operation {
                task: signal.id,
                completion,
                abort,
                cancellable,
            },
        );
        id
    }
    pub fn status(&self, task: u64, id: u64) -> anyhow::Result<i64> {
        let entries = self.entries.lock().unwrap();
        let operation = entries
            .get(&id)
            .ok_or_else(|| anyhow::anyhow!("unknown operation handle"))?;
        anyhow::ensure!(operation.task == task, "operation belongs to another task");
        let state = operation.completion.state.lock().unwrap();
        match &*state {
            CompletionState::Pending => Ok(abi::PENDING),
            CompletionState::Ready(bytes) => Ok(bytes.len() as i64),
            CompletionState::Closed => anyhow::bail!("closed operation"),
        }
    }
    pub fn take(
        &self,
        task: u64,
        id: u64,
        write: impl FnOnce(&[u8]) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let mut entries = self.entries.lock().unwrap();
        let operation = entries
            .get(&id)
            .ok_or_else(|| anyhow::anyhow!("unknown operation handle"))?;
        anyhow::ensure!(operation.task == task, "operation belongs to another task");
        {
            let mut state = operation.completion.state.lock().unwrap();
            let CompletionState::Ready(bytes) = &*state else {
                anyhow::bail!("operation not ready");
            };
            write(bytes)?;
            *state = CompletionState::Closed;
        }
        entries.remove(&id);
        Ok(())
    }
    pub fn release(&self, task: u64, id: u64, cleanup: bool) -> anyhow::Result<()> {
        let mut entries = self.entries.lock().unwrap();
        let operation = entries
            .get(&id)
            .ok_or_else(|| anyhow::anyhow!("unknown operation handle"))?;
        anyhow::ensure!(
            cleanup || operation.task == task,
            "operation belongs to another task"
        );
        let operation = entries.remove(&id).unwrap();
        *operation.completion.state.lock().unwrap() = CompletionState::Closed;
        if operation.cancellable {
            operation.abort.abort();
        }
        Ok(())
    }
    pub async fn finish_task(&self, task: u64) {
        for operation in self
            .entries
            .lock()
            .unwrap()
            .values()
            .filter(|op| op.task == task)
        {
            *operation.completion.state.lock().unwrap() = CompletionState::Closed;
            if operation.cancellable {
                operation.abort.abort();
            }
        }
        let mut jobs = self.jobs.lock().unwrap().remove(&task).unwrap_or_default();
        while let Some(joined) = jobs.join_next().await {
            if let Err(error) = joined {
                assert!(error.is_cancelled(), "host operation panicked: {error}");
            }
        }
    }
    pub async fn quiesce(&self) {
        let tasks: Vec<_> = self.jobs.lock().unwrap().keys().copied().collect();
        for task in tasks {
            self.finish_task(task).await;
        }
    }
    pub async fn finish(&self) {
        self.quiesce().await;
        self.entries.lock().unwrap().clear();
    }
}
