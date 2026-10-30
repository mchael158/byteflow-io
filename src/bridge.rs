//! Bounded [`byteflow::HostAwaitBridge`] backed by a fixed host worker pool.
//!
//! `submit` never blocks a Byteflow worker: jobs enter a bounded channel or
//! fail immediately. Slow work runs on dedicated threads owned by this
//! module. Completions are settled through [`byteflow::finish_host_await`].

use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, Weak};
use std::thread::{self, JoinHandle};

use byteflow::{
    finish_host_await, FlowId, HostAwaitBridge, HostAwaitCompleter, HostAwaitOp, HostAwaitRequest,
};

use crate::{IoDriver, IoDriverError, IoInitError};

/// Cooperative cancellation flag observed by host drivers during HostAwait work.
///
/// The flag alone does not close sockets or abort OS calls. Drivers must poll
/// [`CancellationToken::is_cancelled`] between blocking steps and apply their
/// own deadlines.
#[derive(Clone, Debug, Default)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl CancellationToken {
    /// Returns `true` after the runtime cancelled this ticket or the bridge
    /// received [`HostAwaitBridge::cancel`].
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct AwaitKey {
    flow: FlowId,
    op: HostAwaitOp,
    ticket: u64,
}

struct Job {
    request: HostAwaitRequest,
    completer: HostAwaitCompleter,
    token: CancellationToken,
    key: AwaitKey,
}

struct WorkerPool {
    sender: Mutex<Option<SyncSender<Job>>>,
    workers: Mutex<Vec<JoinHandle<()>>>,
    closed: AtomicBool,
}

impl std::fmt::Debug for WorkerPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerPool")
            .field("closed", &self.closed.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl WorkerPool {
    fn new(
        driver: Arc<dyn IoDriver>,
        worker_count: usize,
        queue_capacity: usize,
        thread_name: &str,
        cancellations: Arc<Mutex<HashMap<AwaitKey, Weak<AtomicBool>>>>,
    ) -> Result<Self, IoInitError> {
        if worker_count == 0 {
            return Err(IoInitError::ZeroWorkers);
        }
        if queue_capacity == 0 {
            return Err(IoInitError::ZeroQueueCapacity);
        }

        let (sender, receiver) = sync_channel::<Job>(queue_capacity);
        let receiver = Arc::new(Mutex::new(receiver));
        let mut workers = Vec::with_capacity(worker_count);

        for index in 0..worker_count {
            let worker_receiver = Arc::clone(&receiver);
            let worker_driver = Arc::clone(&driver);
            let worker_cancellations = Arc::clone(&cancellations);
            let name = format!("{thread_name}-{index}");

            let spawn_result = thread::Builder::new().name(name).spawn(move || {
                worker_loop(worker_receiver, worker_driver, worker_cancellations);
            });

            match spawn_result {
                Ok(handle) => workers.push(handle),
                Err(error) => {
                    drop(sender);
                    for handle in workers {
                        let _ = handle.join();
                    }
                    return Err(IoInitError::ThreadSpawn(error.to_string()));
                }
            }
        }

        Ok(Self {
            sender: Mutex::new(Some(sender)),
            workers: Mutex::new(workers),
            closed: AtomicBool::new(false),
        })
    }

    fn try_send(&self, job: Job) -> Result<(), QueueSubmitError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(QueueSubmitError::Closed(job));
        }

        let sender = match self.sender.lock() {
            Ok(guard) => guard.as_ref().cloned(),
            Err(_) => None,
        };

        let Some(sender) = sender else {
            return Err(QueueSubmitError::Closed(job));
        };

        match sender.try_send(job) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(job)) => Err(QueueSubmitError::Full(job)),
            Err(TrySendError::Disconnected(job)) => Err(QueueSubmitError::Closed(job)),
        }
    }

    fn shutdown(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }

        // Dropping the last sender closes the channel. Workers finish queued
        // jobs, then exit when `recv` returns `Disconnected`.
        let sender = match self.sender.lock() {
            Ok(mut guard) => guard.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        drop(sender);

        let workers = match self.workers.lock() {
            Ok(mut guard) => std::mem::take(&mut *guard),
            Err(poisoned) => std::mem::take(&mut *poisoned.into_inner()),
        };

        let current = thread::current().id();
        for worker in workers {
            // Do not self-join when shutdown is invoked from a worker thread.
            if worker.thread().id() != current {
                let _ = worker.join();
            }
        }
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        match self.sender.get_mut() {
            Ok(sender) => {
                sender.take();
            }
            Err(poisoned) => {
                poisoned.into_inner().take();
            }
        }
        let workers = match self.workers.get_mut() {
            Ok(workers) => std::mem::take(workers),
            Err(poisoned) => std::mem::take(poisoned.into_inner()),
        };
        let current = thread::current().id();
        for worker in workers {
            if worker.thread().id() != current {
                let _ = worker.join();
            }
        }
    }
}

enum QueueSubmitError {
    Full(Job),
    Closed(Job),
}

/// Bounded host worker pool implementing Byteflow’s [`HostAwaitBridge`].
///
/// Suitable as [`byteflow::RuntimeConfig::host_await`]. Queue saturation and
/// shutdown fail the outstanding completer immediately instead of blocking a
/// scheduler worker.
pub struct WorkerHostAwaitBridge {
    pool: WorkerPool,
    cancellations: Arc<Mutex<HashMap<AwaitKey, Weak<AtomicBool>>>>,
}

impl std::fmt::Debug for WorkerHostAwaitBridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerHostAwaitBridge")
            .field("pool", &self.pool)
            .finish_non_exhaustive()
    }
}

impl WorkerHostAwaitBridge {
    /// Spawn `worker_count` host threads behind a channel of `queue_capacity`.
    ///
    /// # Errors
    ///
    /// Returns [`IoInitError`] when either capacity is zero or thread creation
    /// fails. On spawn failure, already-started workers are joined before the
    /// error is returned.
    pub fn new(
        driver: Arc<dyn IoDriver>,
        worker_count: usize,
        queue_capacity: usize,
        thread_name: impl AsRef<str>,
    ) -> Result<Self, IoInitError> {
        let cancellations = Arc::new(Mutex::new(HashMap::new()));
        let pool = WorkerPool::new(
            driver,
            worker_count,
            queue_capacity,
            thread_name.as_ref(),
            Arc::clone(&cancellations),
        )?;

        Ok(Self {
            pool,
            cancellations,
        })
    }

    /// Refuse new submissions, drain accepted work, and join worker threads.
    pub fn shutdown(&self) {
        self.pool.shutdown();
    }

    fn remove_cancellation(&self, key: AwaitKey) {
        if let Ok(mut entries) = self.cancellations.lock() {
            entries.remove(&key);
        }
    }
}

impl HostAwaitBridge for WorkerHostAwaitBridge {
    fn submit(&self, request: HostAwaitRequest, completer: HostAwaitCompleter) {
        let key = AwaitKey {
            flow: request.flow,
            op: request.op,
            ticket: completer.ticket(),
        };
        let token = CancellationToken::default();

        match self.cancellations.lock() {
            Ok(mut entries) => {
                entries.insert(key, Arc::downgrade(&token.cancelled));
            }
            Err(_) => {
                let _ = completer.fail("byteflow-io cancellation registry is poisoned");
                return;
            }
        }

        let job = Job {
            request,
            completer,
            token,
            key,
        };

        match self.pool.try_send(job) {
            Ok(()) => {}
            Err(QueueSubmitError::Full(job)) => {
                self.remove_cancellation(job.key);
                let _ = job.completer.fail("byteflow-io host-await queue is full");
            }
            Err(QueueSubmitError::Closed(job)) => {
                self.remove_cancellation(job.key);
                let _ = job.completer.fail("byteflow-io is shutting down");
            }
        }
    }

    fn cancel(&self, flow: FlowId, op: HostAwaitOp, ticket: u64) {
        let key = AwaitKey { flow, op, ticket };
        let token = match self.cancellations.lock() {
            Ok(mut entries) => entries.remove(&key).and_then(|weak| weak.upgrade()),
            Err(_) => None,
        };
        if let Some(inner) = token {
            CancellationToken { cancelled: inner }.cancel();
        }
    }
}

fn worker_loop(
    receiver: Arc<Mutex<Receiver<Job>>>,
    driver: Arc<dyn IoDriver>,
    cancellations: Arc<Mutex<HashMap<AwaitKey, Weak<AtomicBool>>>>,
) {
    loop {
        let received = match receiver.lock() {
            Ok(guard) => guard.recv(),
            Err(poisoned) => poisoned.into_inner().recv(),
        };

        let job = match received {
            Ok(job) => job,
            Err(_) => return,
        };

        let result = if job.token.is_cancelled() || job.completer.is_cancelled() {
            Err(IoDriverError::new(
                "HostAwait operation cancelled before execution",
            ))
        } else {
            match catch_unwind(AssertUnwindSafe(|| {
                driver.execute_host_await(job.request.op, job.request.args.clone(), &job.token)
            })) {
                Ok(result) => result,
                Err(_) => Err(IoDriverError::new("host I/O driver panicked")),
            }
        };

        // Settle through the core helper so cancel/stale races match Byteflow
        // (`Stale` when the ticket was already finalized).
        let _ = finish_host_await(job.completer, result.map_err(|error| error.to_string()));

        if let Ok(mut entries) = cancellations.lock() {
            entries.remove(&job.key);
        }
    }
}
