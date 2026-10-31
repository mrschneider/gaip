//! A minimal fixed-size thread pool.
//!
//! One native thread per task (8 MiB of stack reserved each) exhausts
//! memory and the scheduler under task-heavy workloads; a fixed pool of
//! workers reuses a bounded set of threads for short blocking jobs.
//! ~60 lines, std only: a shared queue plus a condition variable. The
//! queue is unbounded on purpose here — admission control for task
//! results is the caller's concern, and an executed job that comes back
//! to a full outbox is refused, not lost.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};

type Job = Box<dyn FnOnce() + Send + 'static>;

pub struct Pool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    queue: Mutex<Queue>,
    ready: Condvar,
}

#[derive(Default)]
struct Queue {
    jobs: VecDeque<Job>,
    shutdown: bool,
}

impl Pool {
    /// Spawn `workers` threads that pull jobs until shutdown.
    pub fn new(workers: usize) -> Self {
        let workers = workers.max(1);
        let inner = Arc::new(PoolInner {
            queue: Mutex::new(Queue::default()),
            ready: Condvar::new(),
        });
        for _ in 0..workers {
            let inner = inner.clone();
            std::thread::Builder::new()
                .name("gaip-pool".into())
                .spawn(move || loop {
                    let job = {
                        let mut q = inner.queue.lock().unwrap();
                        loop {
                            if let Some(job) = q.jobs.pop_front() {
                                break job;
                            }
                            if q.shutdown {
                                return;
                            }
                            q = inner.ready.wait(q).unwrap();
                        }
                    };
                    job();
                })
                .expect("spawn pool worker");
        }
        Self { inner }
    }

    /// Worker count recommended for blocking task work: one per core,
    /// times a small over-subscription factor.
    pub fn for_blocking_work() -> Self {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Self::new(cores * 4)
    }

    /// Queue a job. Returns false only after shutdown.
    pub fn submit(&self, job: impl FnOnce() + Send + 'static) -> bool {
        let mut q = self.inner.queue.lock().unwrap();
        if q.shutdown {
            return false;
        }
        q.jobs.push_back(Box::new(job));
        self.inner.ready.notify_one();
        true
    }

    /// Stop accepting jobs; workers drain the queue and exit.
    pub fn shutdown(&self) {
        let mut q = self.inner.queue.lock().unwrap();
        q.shutdown = true;
        self.inner.ready.notify_all();
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[test]
    fn every_submitted_job_runs_exactly_once() {
        let pool = Pool::new(4);
        let n = Arc::new(AtomicUsize::new(0));
        for i in 0..200 {
            let n = n.clone();
            assert!(pool.submit(move || {
                n.fetch_add(1, Ordering::Relaxed);
                if i % 7 == 0 {
                    std::thread::sleep(Duration::from_micros(50));
                }
            }));
        }
        for _ in 0..200 {
            if n.load(Ordering::Relaxed) == 200 {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(n.load(Ordering::Relaxed), 200);
    }

    #[test]
    fn fewer_workers_than_jobs_still_completes_all() {
        let pool = Pool::new(2);
        let n = Arc::new(AtomicUsize::new(0));
        for _ in 0..50 {
            let n = n.clone();
            pool.submit(move || {
                std::thread::sleep(Duration::from_millis(2));
                n.fetch_add(1, Ordering::Relaxed);
            });
        }
        for _ in 0..200 {
            if n.load(Ordering::Relaxed) == 50 {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(n.load(Ordering::Relaxed), 50);
    }

    #[test]
    fn after_shutdown_submissions_are_refused() {
        let pool = Pool::new(1);
        pool.shutdown();
        assert!(!pool.submit(|| {}));
    }
}
