//! Error types for bridge initialization and native-table composition.

use std::fmt;

use byteflow::NativeTableError;

/// Failure while creating the host I/O worker pool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IoInitError {
    /// `worker_threads` was zero.
    ZeroWorkers,
    /// `queue_capacity` was zero.
    ZeroQueueCapacity,
    /// An OS thread could not be spawned.
    ThreadSpawn(String),
}

impl fmt::Display for IoInitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroWorkers => write!(f, "byteflow-io requires at least one worker thread"),
            Self::ZeroQueueCapacity => {
                write!(f, "byteflow-io queue capacity must be greater than zero")
            }
            Self::ThreadSpawn(message) => {
                write!(f, "failed to start byteflow-io worker: {message}")
            }
        }
    }
}

impl std::error::Error for IoInitError {}

/// Failure while composing Byteflow’s standard natives with I/O slots.
#[derive(Debug)]
pub enum NativeTableBuildError {
    /// A required standard native was missing from the core table.
    MissingStandardNative { index: u32, name: &'static str },
    /// [`byteflow::NativeTableBuilder`] rejected a registration.
    NativeTable(NativeTableError),
}

impl fmt::Display for NativeTableBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingStandardNative { index, name } => {
                write!(
                    f,
                    "Byteflow standard native {name} is missing at slot {index}"
                )
            }
            Self::NativeTable(error) => write!(f, "failed to build native table: {error}"),
        }
    }
}

impl std::error::Error for NativeTableBuildError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::MissingStandardNative { .. } => None,
            Self::NativeTable(error) => Some(error),
        }
    }
}

impl From<NativeTableError> for NativeTableBuildError {
    fn from(value: NativeTableError) -> Self {
        Self::NativeTable(value)
    }
}
