//! Stderr is a host sink; recording a runtime log never performs console I/O.
use forest_runtime::LogReader;

pub struct Stderr(tokio::task::JoinHandle<()>);
impl Stderr {
    pub fn start(reader: LogReader) -> Self {
        Self(tokio::spawn(async move {
            let mut updates = reader.subscribe();
            let mut cursor = None;
            loop {
                updates.borrow_and_update();
                let batch = reader.after(cursor);
                cursor = Some(batch.cursor);
                for entry in batch.entries {
                    eprintln!("[{:?}] {}: {}", entry.level, entry.source, entry.message);
                }
                if updates.changed().await.is_err() {
                    break;
                }
            }
        }))
    }
}
impl Drop for Stderr {
    fn drop(&mut self) {
        self.0.abort();
    }
}
