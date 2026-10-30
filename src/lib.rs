#![deny(unsafe_code)]

//! Host-side I/O integration for Byteflow.
//!
//! This crate deliberately does not add I/O instructions to the Byteflow ISA.
//! It provides bounded host queues, fast non-blocking native functions,
//! `HostAwaitBridge` execution, and inbound event helpers.
//!
//! The host owns concrete MQTT/UART/GPIO/payment clients. A driver implements
//! [`IoDriver`] and is responsible for making [`IoDriver::try_submit`] bounded
//! and non-blocking.

mod bridge;
mod driver;
mod error;
mod events;
mod native_table;
mod queue;

pub use bridge::{CancellationToken, WorkerHostAwaitBridge};
pub use driver::{IoCommand, IoDriver, IoDriverError};
pub use error::{IoInitError, NativeTableBuildError};
pub use events::{event_tag, EventRouter};
pub use native_table::{build_native_table, native_index};
pub use queue::{HostAwaitHandler, IoCommandReceiver, QueuedCommand, QueuedIoDriver};

/// Suggested host-owned `HostAwaitOp` values. These are not ISA opcodes.
pub mod host_op {
    pub const MQTT_PUBLISH_AND_WAIT_ACK: u32 = 1;
    pub const PAYMENT_STATUS: u32 = 2;
    pub const START_MACHINE_CYCLE_AND_WAIT: u32 = 3;
    pub const ESP_OTA_CHUNK: u32 = 4;
}

use std::sync::Arc;

use byteflow::{HostAwaitBridge, OutputSink, RuntimeConfig};

/// Default process-wide host-await limit used by this integration layer.
pub const DEFAULT_MAX_HOST_AWAITS: u32 = 128;

/// Configuration for the I/O bridge owned by the host process.
#[derive(Clone, Debug)]
pub struct IoConfig {
    /// Number of dedicated host I/O workers. These are not Byteflow workers.
    pub worker_threads: usize,
    /// Maximum number of queued host-await jobs waiting for a worker.
    pub queue_capacity: usize,
    /// Maximum number of outstanding HostAwait operations allowed by Byteflow.
    pub max_host_awaits: u32,
    /// Name prefix used for host I/O worker threads.
    pub thread_name: String,
}

impl Default for IoConfig {
    fn default() -> Self {
        Self {
            worker_threads: 2,
            queue_capacity: 128,
            max_host_awaits: DEFAULT_MAX_HOST_AWAITS,
            thread_name: "byteflow-io".to_owned(),
        }
    }
}

/// Host-side integration object. It does not own the Byteflow `Runtime`.
///
/// Keep this object alive for as long as the runtime can execute `HostAwait`.
/// Shutdown order is: stop external driver input, call `Runtime::shutdown()`,
/// then call [`ByteflowIo::shutdown`].
pub struct ByteflowIo {
    driver: Arc<dyn IoDriver>,
    bridge: Arc<WorkerHostAwaitBridge>,
    config: IoConfig,
}

impl std::fmt::Debug for ByteflowIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ByteflowIo")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl ByteflowIo {
    /// Create the host I/O bridge and its bounded worker pool.
    pub fn new(driver: Arc<dyn IoDriver>, config: IoConfig) -> Result<Self, IoInitError> {
        let bridge = Arc::new(WorkerHostAwaitBridge::new(
            Arc::clone(&driver),
            config.worker_threads,
            config.queue_capacity,
            config.thread_name.clone(),
        )?);

        Ok(Self {
            driver,
            bridge,
            config,
        })
    }

    /// Return the bridge as Byteflow's public trait object.
    pub fn host_await_bridge(&self) -> Arc<dyn HostAwaitBridge> {
        self.bridge.clone()
    }

    /// Build a `RuntimeConfig` connected to this bridge.
    ///
    /// Pass the same output sink to [`build_native_table`] so the standard
    /// `print` native and the runtime configuration share one sink.
    pub fn runtime_config(&self, output: Arc<dyn OutputSink>) -> RuntimeConfig {
        RuntimeConfig {
            host_await: Some(self.host_await_bridge()),
            max_host_awaits: self.config.max_host_awaits,
            output,
            ..RuntimeConfig::default()
        }
    }

    /// Access the configured driver as a trait object.
    pub fn driver(&self) -> &Arc<dyn IoDriver> {
        &self.driver
    }

    /// Stop accepting bridge work, drain already queued jobs, join workers,
    /// then ask the driver to shut down.
    ///
    /// Call this only after `Runtime::shutdown()` has completed. The driver is
    /// shared with the native functions and should not be stopped earlier.
    pub fn shutdown(&self) -> Result<(), IoDriverError> {
        self.bridge.shutdown();
        self.driver.shutdown()
    }
}

impl Drop for ByteflowIo {
    fn drop(&mut self) {
        // Drop is a best-effort safety net. Call `shutdown` explicitly to
        // observe errors and to make the runtime/driver ordering deliberate.
        self.bridge.shutdown();
    }
}
