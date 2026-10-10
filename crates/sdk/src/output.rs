//! One process-owned bounded stdout worker. A blocked OS write is not cancellable;
//! shutdown waits at most one second, then detaches this single worker until the
//! process exits. No flush/delivery is promised for a full or broken pipe.
use crate::MAX_EVENT_BYTES;
use std::{
    fs::File,
    io::{self, Write},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, Receiver, SyncSender},
    },
    time::Duration,
};
const CAPACITY: usize = 128;
const MAX_COUNTER: u64 = 9_007_199_254_740_991;
fn add(counter: &AtomicU64) {
    let _ = counter.try_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
        Some(v.saturating_add(1).min(MAX_COUNTER))
    });
}

#[derive(Clone, Copy, Debug, Default)]
pub struct OutputHealth {
    pub dropped_events: u64,
    pub pending_events: u64,
    pub write_failures: u64,
    pub terminating: bool,
    pub shutdown_incomplete: bool,
}
#[derive(Default)]
pub(crate) struct State {
    dropped: AtomicU64,
    pending: AtomicU64,
    failures: AtomicU64,
    terminating: AtomicBool,
    incomplete: AtomicBool,
}
impl State {
    pub fn snapshot(&self) -> OutputHealth {
        OutputHealth {
            dropped_events: self.dropped.load(Ordering::Relaxed),
            pending_events: self.pending.load(Ordering::Relaxed),
            write_failures: self.failures.load(Ordering::Relaxed),
            terminating: self.terminating.load(Ordering::Acquire),
            shutdown_incomplete: self.incomplete.load(Ordering::Relaxed),
        }
    }
}
#[derive(Clone)]
pub struct LocalWriter {
    sender: SyncSender<Option<Vec<u8>>>,
    pub(crate) state: Arc<State>,
}
impl Write for LocalWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_EVENT_BYTES + 1 {
            add(&self.state.dropped);
            return Err(io::Error::other("telemetry byte budget"));
        }
        if self.state.terminating.load(Ordering::Acquire) {
            add(&self.state.dropped);
            return Err(io::Error::other("telemetry output terminated"));
        }
        self.state.pending.fetch_add(1, Ordering::Relaxed);
        if self.sender.try_send(Some(bytes.to_vec())).is_err() {
            self.state.pending.fetch_sub(1, Ordering::Relaxed);
            add(&self.state.dropped);
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
/// Separate OS handle avoids holding Rust's global stdout lock, which its process
/// exit cleanup also acquires. This is essential with an undrained stdout pipe.
fn stdout_file() -> io::Result<File> {
    #[cfg(unix)]
    {
        use std::os::fd::AsFd;
        Ok(File::from(io::stdout().as_fd().try_clone_to_owned()?))
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::AsHandle;
        Ok(File::from(io::stdout().as_handle().try_clone_to_owned()?))
    }
}
pub(crate) struct Worker {
    writer: LocalWriter,
    done: Receiver<()>,
    closed: bool,
}
impl Worker {
    pub fn close(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.writer.state.terminating.store(true, Ordering::Release);
        // A full queue already has a wake-up. After draining it the worker sees
        // terminating and exits; producers cannot enqueue after termination.
        let _ = self.writer.sender.try_send(None);
        if self.done.recv_timeout(Duration::from_secs(1)).is_err() {
            self.writer.state.incomplete.store(true, Ordering::Relaxed);
        }
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.close();
    }
}
pub(crate) fn start() -> io::Result<(LocalWriter, Worker)> {
    let mut output = stdout_file()?;
    let (sender, receiver) = mpsc::sync_channel::<Option<Vec<u8>>>(CAPACITY);
    let (done, completion) = mpsc::sync_channel(1);
    let state = Arc::new(State::default());
    let observed = state.clone();
    // Dropping the JoinHandle detaches a potentially stuck OS write. There is
    // one such worker per process, enforced by install's single-use guard.
    let _thread = std::thread::Builder::new()
        .name("nds-log-writer".into())
        .spawn(move || {
            while let Ok(message) = receiver.recv() {
                if let Some(bytes) = message {
                    if output.write_all(&bytes).is_err() {
                        add(&observed.failures);
                        add(&observed.dropped);
                    }
                    observed.pending.fetch_sub(1, Ordering::Relaxed);
                }
                if observed.terminating.load(Ordering::Acquire)
                    && observed.pending.load(Ordering::Relaxed) == 0
                {
                    break;
                }
            }
            if output.flush().is_err() {
                add(&observed.failures);
            }
            let _ = done.try_send(());
        })?;
    let writer = LocalWriter { sender, state };
    Ok((
        writer.clone(),
        Worker {
            writer,
            done: completion,
            closed: false,
        },
    ))
}
