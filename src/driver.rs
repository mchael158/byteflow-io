//! Host driver contract for fast command submission and slow HostAwait work.

use std::error::Error;
use std::fmt;

use byteflow::{HostAwaitOp, Value};

use crate::CancellationToken;

/// Command accepted by a host driver through the fast, non-blocking path.
#[derive(Clone, Debug, PartialEq)]
pub enum IoCommand {
    /// Queue a GPIO output change.
    ///
    /// Hardware access must occur on a driver-owned thread, never on a
    /// Byteflow worker.
    GpioWrite { pin: u32, high: bool },
    /// Queue an MQTT publish.
    ///
    /// The command ID is returned before any broker acknowledgement.
    MqttPublish {
        topic: String,
        payload: Value,
        qos: u8,
    },
    /// Queue a UART write.
    ///
    /// The command ID is returned before transmission completes.
    UartWrite { port: String, bytes: Vec<u8> },
}

/// Error returned by a host I/O adapter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IoDriverError {
    message: String,
}

impl IoDriverError {
    /// Create an error with a human-readable message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// Borrow the error message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for IoDriverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for IoDriverError {}

/// Host application contract for concrete devices and services.
///
/// # Fast path — [`IoDriver::try_submit`]
///
/// Invoked inline by `CallNative`. Implementations must only validate and
/// enqueue into a bounded queue. They must not wait on MQTT, UART, GPIO
/// hardware, payment services, unbounded locks, or network I/O.
///
/// # Slow path — [`IoDriver::execute_host_await`]
///
/// Runs on a dedicated `byteflow-io` worker. Slow I/O is permitted, but
/// implementations must apply finite deadlines and cooperate with
/// [`CancellationToken`].
pub trait IoDriver: Send + Sync + fmt::Debug + 'static {
    /// Submit a fast command and return a host-defined command ID immediately.
    fn try_submit(&self, command: IoCommand) -> Result<u64, IoDriverError>;

    /// Execute a long-running host operation associated with `HostAwait`.
    ///
    /// The default implementation fails closed so an unimplemented operation
    /// cannot succeed silently.
    fn execute_host_await(
        &self,
        _op: HostAwaitOp,
        _args: Value,
        _cancel: &CancellationToken,
    ) -> Result<Value, IoDriverError> {
        Err(IoDriverError::new(
            "HostAwait operation is not implemented by this driver",
        ))
    }

    /// Stop accepting new device work and release host-owned resources.
    ///
    /// Call only after Byteflow has shut down and the `byteflow-io` bridge has
    /// drained its queue.
    fn shutdown(&self) -> Result<(), IoDriverError> {
        Ok(())
    }
}
