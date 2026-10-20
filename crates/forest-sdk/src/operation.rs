use crate::host::imports;
use forest_protocol::abi;
use serde::{Serialize, de::DeserializeOwned};
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll, Waker},
};

pub(crate) type Start = unsafe extern "C" fn(u32, u32) -> u64;

enum State {
    New(Start, Vec<u8>),
    Pending { id: u64, task: u64 },
    Done,
}

std::thread_local! {
    static CURRENT: Cell<Option<u64>> = const { Cell::new(None) };
    static WAITERS: RefCell<HashMap<u64, HashMap<u64,Waker>>> = RefCell::new(HashMap::new());
}

pub(crate) fn enter(task: u64) {
    CURRENT.with(|current| {
        assert!(
            current.replace(Some(task)).is_none(),
            "nested guest task poll"
        );
    });
    // Native completion wakes the root. Propagate it to each operation's actual
    // Rust Waker before polling combinators such as FuturesUnordered.
    let ready = WAITERS.with(|waiters| {
        waiters
            .borrow()
            .get(&task)
            .into_iter()
            .flat_map(|waiters| {
                waiters
                    .iter()
                    .filter(|(id, _)| unsafe { imports::operation_status(**id) } != abi::PENDING)
                    .map(|(_, waker)| waker.clone())
            })
            .collect::<Vec<_>>()
    });
    for waker in ready {
        waker.wake();
    }
}
pub(crate) fn leave(task: u64, complete: bool) {
    CURRENT.with(|current| assert_eq!(current.replace(None), Some(task)));
    if complete {
        WAITERS.with(|waiters| {
            waiters.borrow_mut().remove(&task);
        });
    }
}
fn unregister(task: u64, id: u64) {
    WAITERS.with(|waiters| {
        if let Some(waiters) = waiters.borrow_mut().get_mut(&task) {
            waiters.remove(&id);
        }
    });
}

#[must_use = "host operations run when awaited or polled"]
pub struct Operation<T> {
    state: State,
    result: PhantomData<fn() -> T>,
}

impl<T> Operation<T> {
    pub(crate) fn new(start: Start, input: &impl Serialize) -> Self {
        Self {
            state: State::New(start, abi::encode(input).expect("host input encoding")),
            result: PhantomData,
        }
    }
}

impl<T: DeserializeOwned> Future for Operation<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<T> {
        let this = self.get_mut();
        let (id, task) = match &this.state {
            State::New(start, bytes) => {
                let task =
                    CURRENT.with(|task| task.get().expect("host operation outside a guest task"));
                (
                    unsafe { start(bytes.as_ptr() as u32, bytes.len() as u32) },
                    task,
                )
            }
            State::Pending { id, task } => (*id, *task),
            State::Done => panic!("completed host operation polled"),
        };
        this.state = State::Pending { id, task };
        let length = unsafe { imports::operation_status(id) };
        if length == abi::PENDING {
            WAITERS.with(|waiters| {
                waiters
                    .borrow_mut()
                    .entry(task)
                    .or_default()
                    .insert(id, context.waker().clone());
            });
            return Poll::Pending;
        }
        let length = usize::try_from(length).expect("host result length");
        assert!(
            length <= abi::MAX_FRAME_BYTES,
            "host result exceeds ABI frame limit"
        );
        let mut bytes = vec![0; length];
        unsafe { imports::operation_take(id, bytes.as_mut_ptr() as u32, length as u32) };
        unregister(task, id);
        this.state = State::Done;
        Poll::Ready(abi::decode(&bytes).expect("host result contract"))
    }
}

impl<T> Drop for Operation<T> {
    fn drop(&mut self) {
        if let State::Pending { id, task } = self.state {
            unregister(task, id);
            unsafe { imports::operation_release(id) };
        }
    }
}
