//! Group state changes are synchronous. No mutable borrow crosses a host operation.
use crate::{content::*, memory::RecallLink};
use forest_onebot::forest::GroupMessage;
use forest_serde::{DeriveSchema, DeriveValue};
use serde::{Deserialize, Serialize};
use std::{cell::RefCell, rc::Rc};

#[derive(Clone, Debug, Serialize, Deserialize, DeriveSchema, DeriveValue)]
pub enum Block {
    Message {
        unit: MemoryUnit,
        trigger: bool,
        recall: Option<Recall>,
    },
    Step {
        units: Vec<MemoryUnit>,
        closed: bool,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize, DeriveSchema, DeriveValue)]
pub struct Recall {
    pub ids: Vec<MemoryId>,
    pub seeds: Vec<MemoryId>,
    pub graph_json: String,
}
impl Block {
    pub fn units(&self) -> Vec<&MemoryUnit> {
        match self {
            Self::Message { unit, .. } => vec![unit],
            Self::Step { units, .. } => units.iter().collect(),
        }
    }
    pub fn size(&self) -> usize {
        match self {
            Self::Message { recall, .. } => 1 + recall.as_ref().map_or(0, |r| r.ids.len()),
            Self::Step { units, .. } => units.len(),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, DeriveSchema, DeriveValue)]
pub struct Queued {
    pub message: GroupMessage,
    pub accepted: Option<MemoryUnit>,
    pub trigger: bool,
    pub restored: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize, DeriveSchema, DeriveValue)]
pub struct Snapshot {
    pub group: i64,
    pub bot: i64,
    pub window: Vec<Block>,
    pub pending: Vec<Queued>,
    pub staged: Vec<Queued>,
    pub diagnostics: Vec<String>,
}
pub struct Session {
    pub data: Snapshot,
    pub owned: bool,
    pub working: bool,
    pub error: Option<String>,
}
pub type Shared = Rc<RefCell<Session>>;
impl Session {
    pub fn new(group: i64, bot: i64) -> Self {
        Self {
            data: Snapshot {
                group,
                bot,
                window: vec![],
                pending: vec![],
                staged: vec![],
                diagnostics: vec![],
            },
            owned: false,
            working: false,
            error: None,
        }
    }
    pub fn restore(mut data: Snapshot) -> Self {
        data.staged.append(&mut data.pending);
        data.pending = std::mem::take(&mut data.staged);
        for queued in &mut data.pending {
            queued.trigger = false;
            queued.restored = true;
        }
        Self {
            data,
            owned: false,
            working: false,
            error: None,
        }
    }
    pub fn has_trigger(&self) -> bool {
        self.data.pending.iter().any(|p| p.trigger)
    }
    pub fn stage(&mut self) -> bool {
        assert!(self.data.staged.is_empty());
        let Some(last) = self
            .data
            .pending
            .iter()
            .rposition(|p| p.trigger && p.accepted.is_some())
        else {
            return false;
        };
        self.data.staged = self.data.pending.drain(..=last).collect();
        self.error = None;
        true
    }
    pub fn drain_ordinary(&mut self) {
        assert!(!self.has_trigger());
        let split = self
            .data
            .pending
            .iter()
            .position(|p| p.accepted.is_none())
            .unwrap_or(self.data.pending.len());
        for p in self.data.pending.drain(..split) {
            self.data.window.push(Block::Message {
                unit: p.accepted.unwrap(),
                trigger: false,
                recall: None,
            });
        }
    }
    pub fn clip_prefix(&self, high: usize, low: usize) -> usize {
        let mut count = self.data.window.iter().map(Block::size).sum::<usize>();
        if count <= high {
            return 0;
        }
        let mut prefix = 0;
        // A single oversized/latest block is preserved; there is no hard budget rejection.
        while count > low && prefix + 1 < self.data.window.len() {
            count -= self.data.window[prefix].size();
            prefix += 1;
        }
        prefix
    }
    pub fn diagnostic(&mut self, error: String) {
        if self.data.diagnostics.len() == 32 {
            self.data.diagnostics.remove(0);
        }
        self.data.diagnostics.push(error);
    }
    pub fn links_for(trigger: &MemoryId, seeds: Vec<MemoryId>) -> Vec<RecallLink> {
        seeds
            .into_iter()
            .map(|recalled| RecallLink {
                trigger: trigger.clone(),
                recalled,
            })
            .collect()
    }
}
/// Releasing a task (including AtYield reload) never leaves a dead owner in memory.
pub struct Owner(pub Shared);
impl Drop for Owner {
    fn drop(&mut self) {
        let mut s = self.0.borrow_mut();
        s.owned = false;
        s.working = false;
    }
}
