//! Ready instance polling. A pending Wasmtime future keeps its Store; only its
//! readiness ticket is rotated. No queue lock is held while polling guest code.
use futures::task::{ArcWake, waker_ref};
use indexmap::IndexSet;
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll, Waker},
};

pub(crate) struct RunQueue {
    state: Mutex<State>,
    parallelism: usize,
}
struct State {
    ready: IndexSet<u64>,
    live: HashMap<u64, Arc<Signal>>,
    polling: usize,
}
struct Signal {
    id: u64,
    queue: Weak<RunQueue>,
    waker: Mutex<Option<Waker>>,
}
struct Drive<F> {
    future: Pin<Box<F>>,
    signal: Arc<Signal>,
    queue: Arc<RunQueue>,
}

impl RunQueue {
    pub fn new(parallelism: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                ready: IndexSet::new(),
                live: HashMap::new(),
                polling: 0,
            }),
            parallelism,
        })
    }
    pub fn drive<F: Future>(
        self: &Arc<Self>,
        id: u64,
        future: F,
    ) -> impl Future<Output = F::Output> + use<F> {
        let signal = Arc::new(Signal {
            id,
            queue: Arc::downgrade(self),
            waker: Mutex::new(None),
        });
        let mut state = self.state.lock().unwrap();
        assert!(
            state.live.insert(id, signal.clone()).is_none(),
            "one advancement per instance"
        );
        state.ready.insert(id);
        Drive {
            future: Box::pin(future),
            signal,
            queue: self.clone(),
        }
    }
    fn wake_head(&self) {
        let target = {
            let state = self.state.lock().unwrap();
            if state.polling == self.parallelism {
                None
            } else {
                state.ready.first().map(|id| state.live[id].clone())
            }
        };
        if let Some(target) = target {
            let waker = target.waker.lock().unwrap().clone();
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }
}
impl ArcWake for Signal {
    fn wake_by_ref(signal: &Arc<Self>) {
        let Some(queue) = signal.queue.upgrade() else {
            return;
        };
        {
            let mut state = queue.state.lock().unwrap();
            // A retired advancement cannot make a newer advancement ready.
            if !state
                .live
                .get(&signal.id)
                .is_some_and(|live| Arc::ptr_eq(live, signal))
            {
                return;
            }
            state.ready.insert(signal.id);
        }
        queue.wake_head();
    }
}
impl<F: Future> Future for Drive<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        *this.signal.waker.lock().unwrap() = Some(cx.waker().clone());
        {
            let mut state = this.queue.state.lock().unwrap();
            if state.polling == this.queue.parallelism
                || state.ready.first() != Some(&this.signal.id)
            {
                return Poll::Pending;
            }
            state.ready.shift_remove_index(0);
            state.polling += 1;
        }
        this.queue.wake_head();
        let waker = waker_ref(&this.signal);
        let result = this.future.as_mut().poll(&mut Context::from_waker(&waker));
        {
            this.queue.state.lock().unwrap().polling -= 1;
        }
        this.queue.wake_head();
        result
    }
}
impl<F> Drop for Drive<F> {
    fn drop(&mut self) {
        {
            let mut state = self.queue.state.lock().unwrap();
            state.live.remove(&self.signal.id);
            state.ready.shift_remove(&self.signal.id);
        }
        self.queue.wake_head();
    }
}
