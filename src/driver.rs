use std::error::Error;
use std::fmt;

use byteflow::{HostAwaitOp, Value};

use crate::CancellationToken;

/// Commands accepted by a host driver through a fast, non-blocking queue.
#[derive(Clone, Debug, PartialEq)]
pub enum IoCommand {
    /// Queue a GPIO output change. The actual hardware access occurs on a
    /// driver-owned thread, never on a Byteflow worker.
    GpioWrite { pin: u32, high: bool },
    /// Queue an MQTT publish. The command ID is returned before the broker ACK.
    MqttPublish {
        topic: String,
        payload: Value,
        qos: u8,
    },
    /// Queue a UART write. The command ID is returned before transmission ends.
    UartWrite { port: String, bytes: Vec<u8> },
}

/// Error returned by the host I/O adapter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IoDriverError {
    message: String,
}

impl IoDriverError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

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

/// Contract implemented by the host application for its concrete devices.
///
/// `try_submit` is called inline by `CallNative`; it MUST only validate and
/// enqueue into a bounded queue. It must not wait for MQTT, UART, GPIO hardware,
/// payment services, locks with unbounded contention, or network I/O.
///
/// `execute_host_await` runs on a dedicated `byteflow-io` worker. It may perform
/// slow I/O, but it must apply deadlines and cooperate with cancellation.
pub trait IoDriver: Send + Sync + fmt::Debug + 'static {
    /// Submit a fast command. Return a host-defined command ID immediately.
    fn try_submit(&self, command: IoCommand) -> Result<u64, IoDriverError>;

    /// Execute a long-running host operation associated with `HostAwait`.
    ///
    /// The default fails closed, so an unimplemented host operation cannot
    /// silently succeed.
    fn execute_host_await(
        &self,
        _op: HostAwaitOp,
        _args: Value,
        _cancel: &CancellationToken,
    ) -> Result<Value, IoDriverError> {
        Err(IoDriverError::new("HostAwait operation is not implemented by this driver"))
    }

    /// Stop accepting new device work and release host-owned resources.
    ///
    /// The application must call this only after Byteflow has been shut down
    /// and the `byteflow-io` bridge has drained its queue.
    fn shutdown(&self) -> Result<(), IoDriverError> {
        Ok(())
    }
}
