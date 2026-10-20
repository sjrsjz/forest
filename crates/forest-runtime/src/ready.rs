use forest_protocol::Token;
use indexmap::IndexSet;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::Notify;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) enum Work {
    Start(Token),
    Resume(u64),
}

#[derive(Default)]
struct ReadyState {
    work: IndexSet<Work>,
    tasks: HashMap<u64, Weak<TaskSignal>>,
}

#[derive(Default)]
pub(crate) struct Mailbox {
    state: Mutex<ReadyState>,
    pub changed: Notify,
}

pub(crate) struct TaskSignal {
    pub id: u64,
    mailbox: Weak<Mailbox>,
}

impl Mailbox {
    pub fn start(&self, interface: Token) {
        self.state
            .lock()
            .unwrap()
            .work
            .insert(Work::Start(interface));
        self.changed.notify_one();
    }
    pub fn register(self: &Arc<Self>, id: u64) -> Arc<TaskSignal> {
        let signal = Arc::new(TaskSignal {
            id,
            mailbox: Arc::downgrade(self),
        });
        assert!(
            self.state
                .lock()
                .unwrap()
                .tasks
                .insert(id, Arc::downgrade(&signal))
                .is_none()
        );
        signal
    }
    pub fn wake(&self, id: u64) {
        let mut state = self.state.lock().unwrap();
        if state.tasks.contains_key(&id) {
            state.work.insert(Work::Resume(id));
            self.changed.notify_one();
        }
    }
    pub fn retire(&self, id: u64) {
        let mut state = self.state.lock().unwrap();
        assert!(state.tasks.remove(&id).is_some());
        state.work.shift_remove(&Work::Resume(id));
    }
    pub fn pop(&self) -> Option<Work> {
        self.state.lock().unwrap().work.shift_remove_index(0)
    }
    pub fn consume_wake(&self, id: u64) {
        self.state
            .lock()
            .unwrap()
            .work
            .shift_remove(&Work::Resume(id));
    }
    pub fn contains_wake(&self, id: u64) -> bool {
        self.state.lock().unwrap().work.contains(&Work::Resume(id))
    }
}

impl TaskSignal {
    pub fn notify(&self) {
        if let Some(mailbox) = self.mailbox.upgrade() {
            mailbox.wake(self.id);
        }
    }
}
