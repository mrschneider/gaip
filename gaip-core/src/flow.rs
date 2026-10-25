//! Backpressure: a slow peer must not become a slow server.
//!
//! A blocking `write` on a stalled TCP peer parks the thread that is
//! serving every other participant. GIOP leaves flow control to the
//! transport; here it is explicit: a bounded queue plus a dedicated
//! writer thread. When the queue is full the sender is told so —
//! `SYSTEM_EXCEPTION/Unavailable`, which the caller knows is retryable —
//! instead of being blocked indefinitely.
//!
//! The queue is bounded on purpose. An unbounded queue is not flow
//! control, it is memory exhaustion with extra steps.

use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, SyncSender, TrySendError};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlowError {
    /// The peer is not draining; the message was NOT queued.
    Full { queued: usize },
    /// The connection is gone.
    Closed,
}

impl std::fmt::Display for FlowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full { queued } => write!(
                f,
                "peer is not draining: output queue full ({queued} pending)"
            ),
            Self::Closed => write!(f, "connection closed"),
        }
    }
}

#[derive(Debug, Default)]
pub struct OutboxStats {
    pub sent: u64,
    pub refused: u64,
    pub bytes: u64,
}

/// A bounded outbox with its own writer thread.
pub struct Outbox {
    tx: SyncSender<Vec<u8>>,
    capacity: usize,
    sent: Arc<AtomicU64>,
    refused: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
}

impl Outbox {
    /// Spawn the writer thread around `writer`.
    pub fn spawn<W: Write + Send + 'static>(mut writer: W, capacity: usize) -> Self {
        let capacity = capacity.max(1);
        let (tx, rx) = sync_channel::<Vec<u8>>(capacity);
        let sent = Arc::new(AtomicU64::new(0));
        let bytes = Arc::new(AtomicU64::new(0));
        let s = sent.clone();
        let b = bytes.clone();
        std::thread::spawn(move || {
            while let Ok(chunk) = rx.recv() {
                if writer.write_all(&chunk).is_err() {
                    break; // the peer is gone; the outbox closes
                }
                let _ = writer.flush();
                s.fetch_add(1, Ordering::Relaxed);
                b.fetch_add(chunk.len() as u64, Ordering::Relaxed);
            }
        });
        Self {
            tx,
            capacity,
            sent,
            refused: Arc::new(AtomicU64::new(0)),
            bytes,
        }
    }

    /// Queue bytes without blocking. A full queue is a refusal the caller
    /// must translate into a retryable protocol error.
    pub fn send(&self, bytes: Vec<u8>) -> Result<(), FlowError> {
        match self.tx.try_send(bytes) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                self.refused.fetch_add(1, Ordering::Relaxed);
                Err(FlowError::Full {
                    queued: self.capacity,
                })
            }
            Err(TrySendError::Disconnected(_)) => Err(FlowError::Closed),
        }
    }

    pub fn stats(&self) -> OutboxStats {
        OutboxStats {
            sent: self.sent.load(Ordering::Relaxed),
            refused: self.refused.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
        }
    }
}

/// Credit-based flow control for a *reader* that must not be flooded by
/// a fast producer: each delivery consumes credit, and the receiver
/// replenishes it. This is what a stream of TASK_STREAM frames needs.
#[derive(Debug, Clone)]
pub struct CreditWindow {
    capacity: usize,
    used: usize,
}

impl CreditWindow {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            used: 0,
        }
    }

    /// Can `n` more messages be accepted?
    pub fn can_accept(&self, n: usize) -> bool {
        self.used + n <= self.capacity
    }

    /// Consume credit. Returns false and consumes nothing when the
    /// window is exhausted.
    pub fn take(&mut self, n: usize) -> bool {
        if !self.can_accept(n) {
            return false;
        }
        self.used += n;
        true
    }

    /// Give credit back (the receiver processed `n` messages).
    pub fn release(&mut self, n: usize) {
        self.used = self.used.saturating_sub(n);
    }

    pub fn used(&self) -> usize {
        self.used
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn available(&self) -> usize {
        self.capacity.saturating_sub(self.used)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::channel;
    use std::time::Duration;

    /// A writer that blocks until the test releases it.
    struct SlowWriter {
        gate: std::sync::mpsc::Receiver<()>,
        out: Arc<std::sync::Mutex<Vec<u8>>>,
    }

    impl Write for SlowWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let _ = self.gate.recv_timeout(Duration::from_millis(50));
            self.out.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn a_draining_peer_receives_everything() {
        let out = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (tx, _rx) = channel::<()>();
        struct W(Arc<std::sync::Mutex<Vec<u8>>>);
        impl Write for W {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(b);
                Ok(b.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let ob = Outbox::spawn(W(out.clone()), 16);
        for i in 0..10 {
            ob.send(vec![i as u8]).unwrap();
        }
        for _ in 0..50 {
            if ob.stats().sent == 10 {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(ob.stats().sent, 10);
        assert_eq!(out.lock().unwrap().len(), 10);
        drop(tx);
    }

    #[test]
    fn a_stalled_peer_gets_a_refusal_not_a_blocked_thread() {
        let out = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (tx, _rx) = channel::<()>();
        let ob = Outbox::spawn(
            SlowWriter {
                gate: _rx,
                out: out.clone(),
            },
            2,
        );
        // the writer consumes ~1 message per 50 ms; the queue holds 2
        let mut refused = 0;
        for _ in 0..20 {
            if ob.send(vec![1, 2, 3]).is_err() {
                refused += 1;
            }
        }
        assert!(refused > 0, "a stalled peer must produce refusals");
        assert!(ob.stats().refused > 0);
        let _ = tx.send(());
        // every message is either sent, refused, or still queued (<= capacity)
        let accounted = ob.stats().refused + ob.stats().sent;
        assert!(
            accounted + 2 >= 20,
            "messages must be accounted for (sent {}, refused {}): {accounted}",
            ob.stats().sent,
            ob.stats().refused
        );
    }

    #[test]
    fn a_closed_connection_reports_closed() {
        let (tx, _rx) = channel::<()>();
        struct Failing;
        impl Write for Failing {
            fn write(&mut self, _b: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "gone"))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let ob = Outbox::spawn(Failing, 4);
        // first write fails inside the thread, closing the channel
        let mut saw_closed = false;
        for _ in 0..10 {
            ob.send(vec![0]).ok();
            std::thread::sleep(Duration::from_millis(10));
            if let Err(FlowError::Closed) = ob.send(vec![0]) {
                saw_closed = true;
                break;
            }
        }
        assert!(saw_closed, "a dead peer must surface as Closed");
        drop(tx);
    }

    #[test]
    fn credit_window_accounts_for_every_message() {
        let mut w = CreditWindow::new(3);
        assert!(w.take(2));
        assert!(!w.take(2), "only 1 credit left");
        assert!(w.take(1));
        assert_eq!(w.available(), 0);
        assert!(!w.take(1));
        w.release(2);
        assert_eq!(w.available(), 2);
        assert!(w.take(2));
        w.release(99);
        assert_eq!(w.used(), 0, "release never underflows");
    }
}
