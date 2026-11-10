use crate::{Handlers, Module};
#[cfg(target_arch = "wasm32")]
use forest_protocol::abi;
use forest_protocol::{AsValue, CreateContext, Invocation, Value, abi::Step};
#[cfg(target_arch = "wasm32")]
use serde::de::DeserializeOwned;
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    rc::Rc,
    sync::Arc,
    task::{Wake, Waker},
};

pub type Task<M> = Pin<Box<dyn Future<Output = Completion<M>>>>;
pub enum Completion<M: Module> {
    Created(Result<M, M::LifecycleError>),
    Output(Value),
    Destroyed(Result<(), M::LifecycleError>),
}

pub struct Runtime<M: Module> {
    module: Option<Rc<M>>,
    handlers: Handlers<M>,
    tasks: BTreeMap<u64, Task<M>>,
}

impl<M: Module> Default for Runtime<M> {
    fn default() -> Self {
        Self::new()
    }
}
impl<M: Module> Runtime<M> {
    pub fn new() -> Self {
        Self {
            module: None,
            handlers: M::handlers(),
            tasks: BTreeMap::new(),
        }
    }
    pub fn create(&mut self, id: u64, context: CreateContext) {
        assert!(
            self.module.is_none() && self.tasks.is_empty(),
            "duplicate create"
        );
        self.put(
            id,
            Box::pin(async move { Completion::Created(M::create(context).await) }),
        );
    }
    pub fn invoke(&mut self, id: u64, invocation: Invocation) {
        let module = self
            .module
            .as_ref()
            .expect("invoke before initialization")
            .clone();
        let future = self.handlers.invoke(module, invocation);
        self.put(
            id,
            Box::pin(async move { Completion::Output(future.await) }),
        );
    }
    pub fn destroy(&mut self, id: u64) -> BTreeMap<u64, Task<M>> {
        let retired = std::mem::take(&mut self.tasks);
        let module = self.module.take().expect("destroy before initialization");
        self.put(
            id,
            Box::pin(async move { Completion::Destroyed(module.destroy().await) }),
        );
        retired
    }
    pub fn take(&mut self, id: u64) -> Task<M> {
        self.tasks.remove(&id).expect("unknown ABI task")
    }
    pub fn put(&mut self, id: u64, task: Task<M>) {
        assert!(self.tasks.insert(id, task).is_none(), "duplicate ABI task");
    }
    pub fn finish(&mut self, result: Completion<M>) -> Step {
        match result {
            Completion::Created(Ok(module)) => {
                self.module = Some(Rc::new(module));
                Step::Lifecycle(Ok(()))
            }
            Completion::Created(Err(error)) => Step::Lifecycle(Err(error.as_value())),
            Completion::Output(value) => Step::Output(value),
            Completion::Destroyed(result) => {
                Step::Lifecycle(result.map_err(|error| error.as_value()))
            }
        }
    }
}

struct TaskWake(u64);
impl Wake for TaskWake {
    fn wake(self: Arc<Self>) {
        unsafe { crate::host::imports::task_wake(self.0) };
    }
}
pub fn waker(id: u64) -> Waker {
    Waker::from(Arc::new(TaskWake(id)))
}
pub fn enter_task(id: u64) {
    crate::operation::enter(id);
}
pub fn leave_task(id: u64, complete: bool) {
    crate::operation::leave(id, complete);
}

#[cfg(target_arch = "wasm32")]
pub fn allocate(len: u32) -> u32 {
    Box::into_raw(vec![0u8; len as usize].into_boxed_slice()) as *mut u8 as u32
}

/// # Safety
/// The host returns exactly one live allocation with its original length.
#[cfg(target_arch = "wasm32")]
pub unsafe fn deallocate(ptr: u32, len: u32) {
    unsafe {
        drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(
            ptr as *mut u8,
            len as usize,
        )))
    };
}

/// # Safety
/// The host supplied a live, initialized byte range for this ABI call.
#[cfg(target_arch = "wasm32")]
pub unsafe fn read_input<T: DeserializeOwned>(ptr: u32, len: u32) -> T {
    abi::decode(unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) })
        .expect("ABI input contract")
}

#[cfg(target_arch = "wasm32")]
pub fn encode_buffer(step: &Step) -> u64 {
    let bytes = abi::encode(step)
        .unwrap_or_else(|_| abi::encode(&Step::FrameTooLarge).expect("fixed ABI control frame"))
        .into_boxed_slice();
    let len = bytes.len() as u32;
    abi::buffer(Box::into_raw(bytes) as *mut u8 as u32, len)
}
