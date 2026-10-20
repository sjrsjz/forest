//! Typed guest SDK. Futures stay in the Wasm instance.
mod handlers;
pub mod host;
mod operation;
#[doc(hidden)]
pub mod runtime;

pub use forest_protocol::{
    self as protocol, AsSchema, AsValue, CallContext, CreateContext, DispatchError, DispatchResult,
    Invocation, Token, Value,
};
pub use forest_serde::{DeriveSchema, DeriveValue};
pub use handlers::Handlers;
pub use operation::Operation;

use std::{future::Future, rc::Rc};

/// An owned Rc enters each async handler: no whole-module mutable borrow is
/// retained across a yield. State mutation uses ordinary scoped Rust borrowing.
pub trait Module: Sized + 'static {
    fn create(context: CreateContext) -> impl Future<Output = Result<Self, String>> + 'static;
    fn handlers() -> Handlers<Self>;
    fn destroy(self: Rc<Self>) -> impl Future<Output = Result<(), String>> + 'static {
        async { Ok(()) }
    }
}

/// Installs fixed exports; the external manifest remains the contract authority.
#[macro_export]
macro_rules! export_module {
    ($module:ty) => {
        #[cfg(target_arch = "wasm32")]
        mod forest_exports {
            use std::{
                cell::RefCell,
                task::{Context, Poll},
            };
            use $crate::{
                protocol::{CreateContext, Invocation, abi},
                runtime::{Runtime, encode_buffer, read_input},
            };
            std::thread_local! {
                static STATE: RefCell<Runtime<$module>> = RefCell::new(Runtime::new());
            }
            #[unsafe(no_mangle)]
            pub extern "C" fn forest_alloc(len: u32) -> u32 {
                $crate::runtime::allocate(len)
            }
            #[unsafe(no_mangle)]
            pub unsafe extern "C" fn forest_free(ptr: u32, len: u32) {
                unsafe { $crate::runtime::deallocate(ptr, len) }
            }
            #[unsafe(no_mangle)]
            pub unsafe extern "C" fn forest_create(task: u64, ptr: u32, len: u32) {
                let context: CreateContext = unsafe { read_input(ptr, len) };
                STATE.with(|s| s.borrow_mut().create(task, context));
            }
            #[unsafe(no_mangle)]
            pub unsafe extern "C" fn forest_invoke(task: u64, ptr: u32, len: u32) {
                let invocation: Invocation = unsafe { read_input(ptr, len) };
                STATE.with(|s| s.borrow_mut().invoke(task, invocation));
            }
            #[unsafe(no_mangle)]
            pub extern "C" fn forest_destroy(task: u64) {
                let retired = STATE.with(|s| s.borrow_mut().destroy(task));
                drop(retired);
            }
            #[unsafe(no_mangle)]
            pub extern "C" fn forest_poll(task: u64) -> u64 {
                $crate::runtime::enter_task(task);
                let mut future = STATE.with(|s| s.borrow_mut().take(task));
                let waker = $crate::runtime::waker(task);
                let result = future.as_mut().poll(&mut Context::from_waker(&waker));
                let step = match result {
                    Poll::Pending => {
                        STATE.with(|s| s.borrow_mut().put(task, future));
                        abi::Step::Pending
                    }
                    Poll::Ready(result) => STATE.with(|s| s.borrow_mut().finish(result)),
                };
                $crate::runtime::leave_task(task, !matches!(step, abi::Step::Pending));
                encode_buffer(&step)
            }
        }
    };
}
