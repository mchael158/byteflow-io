use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, RecvError, RecvTimeoutError, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use byteflow::{HostAwaitOp, Value};

use crate::{CancellationToken, IoCommand, IoDriver, IoDriverError};

/// A fast I/O command together with the host-side ID returned to bytecode.
#[derive(Clone, Debug, PartialEq)]
pub struct QueuedCommand {
    pub id: u64,
    pub command: IoCommand,
}

/// Host-specific executor for slow operations reached through `HostAwait`.
pub trait HostAwaitHandler: Send + Sync + std::fmt::Debug + 'static {
    fn execute(
        &self,
        op: HostAwaitOp,
        args: Value,
        cancel: &CancellationToken,
    ) -> Result<Value, IoDriverError>;

    fn shutdown(&self) -> Result<(), IoDriverError> {
        Ok(())
    }
}

/// Bounded command receiver owned by a host device loop.
pub struct IoCommandReceiver {
    receiver: Mutex<Receiver<QueuedCommand>>,
}

impl std::fmt::Debug for IoCommandReceiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IoCommandReceiver").finish_non_exhaustive()
    }
}

impl IoCommandReceiver {
    /// Wait for one command. This is intended for a host-owned driver thread,
    /// never for a Byteflow worker.
    pub fn recv(&self) -> Result<QueuedCommand, RecvError> {
        match self.receiver.lock() {
            Ok(receiver) => receiver.recv(),
            Err(_) => Err(RecvError),
        }
    }

    /// Wait for one command up to `timeout` on a host-owned driver thread.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<QueuedCommand, RecvTimeoutError> {
        match self.receiver.lock() {
            Ok(receiver) => receiver.recv_timeout(timeout),
            Err(_) => Err(RecvTimeoutError::Disconnected),
        }
    }

    /// Non-blocking poll for a host-owned driver loop.
    pub fn try_recv(&self) -> Result<QueuedCommand, TryRecvError> {
        match self.receiver.lock() {
            Ok(receiver) => receiver.try_recv(),
            Err(_) => Err(TryRecvError::Disconnected),
        }
    }
}

/// A ready-to-use bounded `IoDriver` implementation.
///
/// It provides the non-blocking fast-command queue. The application consumes
/// [`IoCommandReceiver`] on its own MQTT/UART/GPIO thread and performs the
/// concrete device operation there. Slow request/response work is delegated to
/// [`HostAwaitHandler`].
pub struct QueuedIoDriver {
    sender: SyncSender<QueuedCommand>,
    next_id: AtomicU64,
    closed: AtomicBool,
    host_await: Arc<dyn HostAwaitHandler>,
}

impl std::fmt::Debug for QueuedIoDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueuedIoDriver")
            .field("closed", &self.closed.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl QueuedIoDriver {
    /// Create a bounded driver queue and return its host-side receiver.
    pub fn new(
        capacity: usize,
        host_await: Arc<dyn HostAwaitHandler>,
    ) -> Result<(Arc<Self>, IoCommandReceiver), IoDriverError> {
        if capacity == 0 {
            return Err(IoDriverError::new("I/O command queue capacity must be greater than zero"));
        }

        let (sender, receiver) = sync_channel(capacity);
        let driver = Arc::new(Self {
            sender,
            next_id: AtomicU64::new(1),
            closed: AtomicBool::new(false),
            host_await,
        });
        let receiver = IoCommandReceiver {
            receiver: Mutex::new(receiver),
        };
        Ok((driver, receiver))
    }
}

impl IoDriver for QueuedIoDriver {
    fn try_submit(&self, command: IoCommand) -> Result<u64, IoDriverError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(IoDriverError::new("I/O command queue is closed"));
        }

        let id = self
            .next_id
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| current.checked_add(1))
            .map_err(|_| IoDriverError::new("I/O command ID space exhausted"))?;

        match self.sender.try_send(QueuedCommand { id, command }) {
            Ok(()) => Ok(id),
            Err(TrySendError::Full(_)) => Err(IoDriverError::new("I/O command queue is full")),
            Err(TrySendError::Disconnected(_)) => {
                Err(IoDriverError::new("I/O command receiver was dropped"))
            }
        }
    }

    fn execute_host_await(
        &self,
        op: HostAwaitOp,
        args: Value,
        cancel: &CancellationToken,
    ) -> Result<Value, IoDriverError> {
        self.host_await.execute(op, args, cancel)
    }

    fn shutdown(&self) -> Result<(), IoDriverError> {
        self.closed.store(true, Ordering::Release);
        self.host_await.shutdown()
    }
}
