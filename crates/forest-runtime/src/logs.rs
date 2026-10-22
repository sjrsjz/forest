//! Bounded host log journal. Readers can replay entries without retaining Core.
use forest_protocol::{Token, service::LogLevel};
use serde::Serialize;
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::watch;

const MAX_ENTRIES: usize = 2000;
const MAX_BYTES: usize = 4 * 1024 * 1024;
const MAX_MESSAGE_BYTES: usize = 16 * 1024;

#[derive(Clone, Debug, Serialize)]
pub struct LogEntry {
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub level: LogLevel,
    pub source: Token,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct LogBatch {
    pub cursor: u64,
    /// The requested prefix expired, or the host restarted with a new sequence.
    pub reset: bool,
    pub entries: Vec<LogEntry>,
}

#[derive(Clone)]
pub struct LogWriter(Arc<Journal>);
#[derive(Clone)]
pub struct LogReader(Arc<Journal>);
struct Journal {
    id: String,
    state: Mutex<State>,
    changed: watch::Sender<u64>,
}
#[derive(Default)]
struct State {
    sequence: u64,
    bytes: usize,
    entries: VecDeque<LogEntry>,
}

impl Default for LogWriter {
    fn default() -> Self {
        static NEXT_JOURNAL: AtomicU64 = AtomicU64::new(1);
        Self(Arc::new(Journal {
            id: format!(
                "{:x}-{}-{}",
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos(),
                std::process::id(),
                NEXT_JOURNAL.fetch_add(1, Ordering::Relaxed)
            ),
            state: Mutex::default(),
            changed: watch::channel(0).0,
        }))
    }
}
impl LogWriter {
    pub fn reader(&self) -> LogReader {
        LogReader(self.0.clone())
    }
    pub fn write(&self, level: LogLevel, source: Token, message: impl Into<String>) {
        let mut message = message.into();
        if message.len() > MAX_MESSAGE_BYTES {
            let mut end = MAX_MESSAGE_BYTES - 3;
            while !message.is_char_boundary(end) {
                end -= 1;
            }
            message.truncate(end);
            message.push('…');
        }
        let mut state = self.0.state.lock().unwrap();
        state.sequence = state
            .sequence
            .checked_add(1)
            .expect("log sequence exhausted");
        let entry = LogEntry {
            sequence: state.sequence,
            timestamp_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            level,
            source,
            message,
        };
        state.bytes += entry.message.len() + entry.source.to_string().len();
        state.entries.push_back(entry);
        while state.entries.len() > MAX_ENTRIES || state.bytes > MAX_BYTES {
            let entry = state.entries.pop_front().unwrap();
            state.bytes -= entry.message.len() + entry.source.to_string().len();
        }
        // Publish under the same lock so concurrent writers cannot regress the cursor.
        self.0.changed.send_replace(state.sequence);
    }
}
impl LogReader {
    pub fn id(&self) -> &str {
        &self.0.id
    }
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.0.changed.subscribe()
    }
    pub fn after(&self, cursor: Option<u64>) -> LogBatch {
        let state = self.0.state.lock().unwrap();
        let oldest = state
            .entries
            .front()
            .map_or(state.sequence + 1, |entry| entry.sequence);
        let reset = cursor.is_none_or(|n| n > state.sequence || n < oldest.saturating_sub(1));
        LogBatch {
            cursor: state.sequence,
            reset,
            entries: state
                .entries
                .iter()
                .filter(|entry| reset || entry.sequence > cursor.unwrap_or(0))
                .cloned()
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_is_ordered_bounded_and_resets_an_expired_cursor() {
        let writer = LogWriter::default();
        let reader = writer.reader();
        let source: Token = "/core/test".parse().unwrap();
        for i in 0..MAX_ENTRIES + 2 {
            writer.write(LogLevel::Debug, source.clone(), i.to_string());
        }
        let batch = reader.after(Some(1));
        assert!(batch.reset);
        assert_eq!(batch.entries.len(), MAX_ENTRIES);
        assert_eq!(batch.entries[0].sequence, 3);
        let last = reader.after(Some(batch.cursor - 1));
        assert!(!last.reset);
        assert_eq!(last.entries.len(), 1);
        assert_eq!(last.entries[0].sequence, batch.cursor);
        assert!(reader.after(Some(batch.cursor)).entries.is_empty());
        assert!(reader.after(Some(batch.cursor + 1)).reset);
    }
}
