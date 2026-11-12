use crate::ready::TaskSignal;
use forest_protocol::{DispatchError, DispatchResult, Token, abi};
use serde::Serialize;
use std::{
    collections::HashMap,
    future::Future,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll, Waker},
};
use tokio::task::{AbortHandle, JoinSet};

enum CompletionState {
    Pending,
    Ready(Completed),
    Closed,
}
struct Completed {
    bytes: Vec<u8>,
    permit: Option<crate::locks::Permit>,
}
impl Completed {
    fn value(value: impl Serialize) -> Self {
        let bytes = abi::encode(&value).unwrap_or_else(|_| {
            abi::encode(&Err::<(), _>(DispatchError::FrameTooLarge)).expect("fixed control frame")
        });
        Self {
            bytes,
            permit: None,
        }
    }
}
struct Completion {
    state: Mutex<CompletionState>,
    signal: Weak<TaskSignal>,
}
struct Operation {
    task: u64,
    completion: Arc<Completion>,
    abort: Option<AbortHandle>,
    cancellable: bool,
}

#[derive(Default)]
pub(crate) struct Operations {
    locks: Arc<crate::locks::Locks>,
    next: AtomicU64,
    entries: Mutex<HashMap<u64, Operation>>,
    jobs: Mutex<HashMap<u64, JoinSet<()>>>,
    held: Mutex<HashMap<u64, HashMap<Token, crate::locks::Permit>>>,
}

impl Operations {
    pub fn start<T: Serialize + Send + 'static>(
        &self,
        signal: &Arc<TaskSignal>,
        cancellable: bool,
        future: impl Future<Output = T> + Send + 'static,
    ) -> u64 {
        self.start_completed(signal, cancellable, async move {
            Completed::value(future.await)
        })
    }

    pub fn lock(
        &self,
        signal: &Arc<TaskSignal>,
        key: Token,
        permission: DispatchResult<()>,
    ) -> u64 {
        let locks = self.locks.clone();
        let future = async move {
            permission?;
            Ok::<_, DispatchError>(locks.acquire(key).await)
        };
        // Register with the FIFO mutex during the import, before another task
        // can submit its lock. A pending future is then polled by its host job.
        let mut future = Box::pin(tokio::task::unconstrained(future));
        match future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(Ok(permit)) => {
                return self.ready(
                    signal,
                    Completed {
                        bytes: abi::encode(&Ok::<(), DispatchError>(()))
                            .expect("fixed lock result"),
                        permit: Some(permit),
                    },
                );
            }
            Poll::Ready(Err(error)) => {
                return self.ready(signal, Completed::value(Err::<(), _>(error)));
            }
            Poll::Pending => {}
        }
        self.start_completed(signal, true, async move {
            match future.await {
                Ok(permit) => Completed {
                    bytes: abi::encode(&Ok::<(), DispatchError>(())).expect("fixed lock result"),
                    permit: Some(permit),
                },
                Err(error) => Completed::value(Err::<(), _>(error)),
            }
        })
    }

    // Unlock has no asynchronous work. Keeping its response in the usual result
    // slot lets every language use the existing lossless Result wire format.
    pub fn unlock(
        &self,
        signal: &Arc<TaskSignal>,
        key: &Token,
        permission: DispatchResult<()>,
    ) -> u64 {
        let result = permission.and_then(|_| {
            let permit = self
                .held
                .lock()
                .unwrap()
                .get_mut(&signal.id)
                .and_then(|locks| locks.remove(key))
                .ok_or(DispatchError::Denied)?;
            drop(permit);
            Ok(())
        });
        self.ready(signal, Completed::value(result))
    }

    fn ready(&self, signal: &Arc<TaskSignal>, result: Completed) -> u64 {
        let id = self.next_id();
        self.entries.lock().unwrap().insert(
            id,
            Operation {
                task: signal.id,
                completion: Arc::new(Completion {
                    state: Mutex::new(CompletionState::Ready(result)),
                    signal: Arc::downgrade(signal),
                }),
                abort: None,
                cancellable: true,
            },
        );
        id
    }

    fn next_id(&self) -> u64 {
        self.next
            .fetch_add(1, Ordering::Relaxed)
            .checked_add(1)
            .expect("operation id exhaustion")
    }

    fn start_completed(
        &self,
        signal: &Arc<TaskSignal>,
        cancellable: bool,
        future: impl Future<Output = Completed> + Send + 'static,
    ) -> u64 {
        let id = self.next_id();
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
            let mut state = result.state.lock().unwrap();
            if matches!(*state, CompletionState::Closed) {
                return;
            }
            assert!(matches!(*state, CompletionState::Pending));
            *state = CompletionState::Ready(output);
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
                abort: Some(abort),
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
            CompletionState::Ready(result) => Ok(result.bytes.len() as i64),
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
            let CompletionState::Ready(result) = &*state else {
                anyhow::bail!("operation not ready");
            };
            write(&result.bytes)?;
            let CompletionState::Ready(result) =
                std::mem::replace(&mut *state, CompletionState::Closed)
            else {
                unreachable!("ready completion is consumed under its lock")
            };
            if let Some(permit) = result.permit {
                let previous = self
                    .held
                    .lock()
                    .unwrap()
                    .entry(task)
                    .or_default()
                    .insert(permit.key.clone(), permit);
                assert!(previous.is_none(), "nonrecursive lock cannot be held twice");
            }
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
        if operation.cancellable
            && let Some(abort) = operation.abort
        {
            abort.abort();
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
            if operation.cancellable
                && let Some(abort) = &operation.abort
            {
                abort.abort();
            }
        }
        let mut jobs = self.jobs.lock().unwrap().remove(&task).unwrap_or_default();
        while let Some(joined) = jobs.join_next().await {
            if let Err(error) = joined {
                assert!(error.is_cancelled(), "host operation panicked: {error}");
            }
        }
        // Submitted, non-cancellable writes have ended before these permits can
        // pass to another task or module. No guest code runs during this cleanup.
        self.held.lock().unwrap().remove(&task);
    }
    pub async fn quiesce(&self) {
        let mut tasks: std::collections::HashSet<_> =
            self.jobs.lock().unwrap().keys().copied().collect();
        tasks.extend(self.held.lock().unwrap().keys().copied());
        tasks.extend(
            self.entries
                .lock()
                .unwrap()
                .values()
                .map(|operation| operation.task),
        );
        for task in tasks {
            self.finish_task(task).await;
        }
    }
    pub async fn finish(&self) {
        self.quiesce().await;
        self.entries.lock().unwrap().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ready::Mailbox;

    fn task(id: u64) -> (Arc<Mailbox>, Arc<TaskSignal>) {
        let mailbox = Arc::new(Mailbox::default());
        let signal = mailbox.register(id);
        (mailbox, signal)
    }

    fn lock(operations: &Operations, signal: &Arc<TaskSignal>, key: &str) -> u64 {
        operations.lock(signal, key.parse().unwrap(), Ok(()))
    }

    fn take(operations: &Operations, task: u64, id: u64) -> DispatchResult<()> {
        let mut result = None;
        operations
            .take(task, id, |bytes| {
                result = Some(abi::decode::<DispatchResult<()>>(bytes)?);
                Ok(())
            })
            .unwrap();
        result.unwrap()
    }

    async fn ready(operations: &Operations, task: u64, id: u64) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while operations.status(task, id).unwrap() == abi::PENDING {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn mutex_fifo_cancellation_and_task_delivery_preserve_the_permit() {
        let instance = Operations::default();
        let (_a, a) = task(1);
        let (_b, b) = task(2);
        let (_c, c) = task(3);
        let key: Token = "/resource".parse().unwrap();
        take(&instance, a.id, lock(&instance, &a, "/resource")).unwrap();
        let cancelled = lock(&instance, &b, "/resource");
        let next = lock(&instance, &c, "/resource");
        assert_eq!(instance.status(b.id, cancelled).unwrap(), abi::PENDING);
        assert_eq!(instance.status(c.id, next).unwrap(), abi::PENDING);
        let denied = instance.unlock(&c, &key, Ok(()));
        assert_eq!(take(&instance, c.id, denied), Err(DispatchError::Denied));
        instance.release(b.id, cancelled, false).unwrap();
        instance.finish_task(b.id).await;
        let release = instance.unlock(&a, &key, Ok(()));
        take(&instance, a.id, release).unwrap();
        ready(&instance, c.id, next).await;
        // Cancelling a granted but undelivered result releases the permit.
        instance.release(c.id, next, false).unwrap();
        instance.finish_task(c.id).await;
        let final_lock = lock(&instance, &b, "/resource");
        ready(&instance, b.id, final_lock).await;
        take(&instance, b.id, final_lock).unwrap();
        instance.finish().await;
    }

    #[tokio::test]
    async fn mutex_task_cleanup_keeps_the_lock_until_submitted_writes_finish() {
        let instance = Arc::new(Operations::default());
        let (_a, a) = task(1);
        let (_b, b) = task(2);
        take(&instance, a.id, lock(&instance, &a, "/write")).unwrap();
        let (finish_write, write) = tokio::sync::oneshot::channel();
        instance.start(&a, false, async move {
            write.await.unwrap();
            Ok::<_, DispatchError>(())
        });
        let cleanup = instance.clone();
        let task = a.id;
        let cleanup = tokio::spawn(async move { cleanup.finish_task(task).await });
        let waiting = lock(&instance, &b, "/write");
        assert_eq!(instance.status(b.id, waiting).unwrap(), abi::PENDING);
        assert!(!cleanup.is_finished());
        finish_write.send(()).unwrap();
        cleanup.await.unwrap();
        ready(&instance, b.id, waiting).await;
        take(&instance, b.id, waiting).unwrap();
        instance.finish().await;
    }

    #[tokio::test]
    async fn mutex_tables_are_instance_local_and_cleanup_is_isolated() {
        let instance = Operations::default();
        let other = Operations::default();
        let (_a, a) = task(1);
        let (_b, b) = task(2);
        let (_c, c) = task(3);
        let (_d, d) = task(1); // Same local task number in another instance.
        take(&instance, a.id, lock(&instance, &a, "/a")).unwrap();
        take(&instance, b.id, lock(&instance, &b, "/b")).unwrap();
        lock(&instance, &c, "/a");
        // Identical names in another instance are immediately independent.
        take(&other, d.id, lock(&other, &d, "/a")).unwrap();
        take(&other, d.id, lock(&other, &d, "/b")).unwrap();
        let waiting = lock(&other, &b, "/a");
        instance.finish().await;
        assert!(instance.held.lock().unwrap().is_empty());
        assert!(instance.entries.lock().unwrap().is_empty());
        assert_eq!(other.status(b.id, waiting).unwrap(), abi::PENDING);
        let release = other.unlock(&d, &"/a".parse().unwrap(), Ok(()));
        take(&other, d.id, release).unwrap();
        ready(&other, b.id, waiting).await;
        take(&other, b.id, waiting).unwrap();
        other.finish().await;
    }
}
