use std::fmt;

use byteflow::NativeTableError;

/// Errors while creating the host I/O worker pool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IoInitError {
    ZeroWorkers,
    ZeroQueueCapacity,
    ThreadSpawn(String),
}

impl fmt::Display for IoInitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroWorkers => write!(f, "byteflow-io requires at least one worker thread"),
            Self::ZeroQueueCapacity => write!(f, "byteflow-io queue capacity must be greater than zero"),
            Self::ThreadSpawn(message) => write!(f, "failed to start byteflow-io worker: {message}"),
        }
    }
}

impl std::error::Error for IoInitError {}

/// Failure while composing the stable standard native table with I/O natives.
#[derive(Debug)]
pub enum NativeTableBuildError {
    MissingStandardNative { index: u32, name: &'static str },
    NativeTable(NativeTableError),
}

impl fmt::Display for NativeTableBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingStandardNative { index, name } => {
                write!(f, "Byteflow standard native {name} is missing at slot {index}")
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
