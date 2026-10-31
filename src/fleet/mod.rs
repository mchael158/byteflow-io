//! Generic low-cost device fleet — not tied to washers or any one product.
//!
//! byteflow-io stays a thin host bridge: Byteflow Runtime + pluggable
//! [`DeviceTransport`]. Change only [`FleetProfile`] (capacity, topic root,
//! label) to retarget lockers, sensors, kiosks, laundry, etc.
//!
//! Cost posture:
//! - no MQTT / hardware SDK in this crate (host plugs one in)
//! - in-memory transport for tests and dry-runs
//! - shared registry + one driver for the whole site
//! - ESP32 (or any MCU) stays optional at the edge

mod device;
mod driver;
mod profile;
mod protocol;
mod topics;
mod transport;

pub use device::{DeviceId, DeviceSlot, DeviceState, FleetError, FleetRegistry, SharedFleet};
pub use driver::{FleetDriver, DEFAULT_DEVICE_DEADLINE};
pub use profile::{FleetProfile, ProfileError};
pub use protocol::{custom_op, event_tag, host_op, DeviceCommand};
pub use topics::{ack, cmd, ota, parse_device, status};
pub use transport::{DeviceTransport, InMemoryDeviceTransport};

use std::sync::Arc;
use std::time::Duration;

use crate::{ByteflowIo, CancellationToken, IoConfig, IoInitError};

/// Wire a profiled fleet into [`ByteflowIo`] with the in-memory transport.
///
/// Swap [`InMemoryDeviceTransport`] for a production MQTT/UART impl without
/// changing Byteflow contracts.
pub fn bootstrap(
    profile: FleetProfile,
    config: IoConfig,
) -> Result<(ByteflowIo, SharedFleet, Arc<FleetDriver>), BootstrapError> {
    bootstrap_with_delay(profile, config, Duration::ZERO)
}

/// Same as [`bootstrap`], with artificial transport delay (cancel / deadline tests).
pub fn bootstrap_with_delay(
    profile: FleetProfile,
    config: IoConfig,
    delay: Duration,
) -> Result<(ByteflowIo, SharedFleet, Arc<FleetDriver>), BootstrapError> {
    let fleet = Arc::new(FleetRegistry::new(profile)?);
    let transport = if delay.is_zero() {
        InMemoryDeviceTransport::new(Arc::clone(&fleet))
    } else {
        InMemoryDeviceTransport::with_delay(Arc::clone(&fleet), delay)
    };
    let driver = FleetDriver::new(Arc::clone(&fleet), transport);
    let io = ByteflowIo::new(Arc::clone(&driver) as Arc<dyn crate::IoDriver>, config)?;
    Ok((io, fleet, driver))
}

/// Errors from fleet bootstrap.
#[derive(Debug)]
pub enum BootstrapError {
    Fleet(FleetError),
    Io(IoInitError),
}

impl std::fmt::Display for BootstrapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fleet(e) => write!(f, "{e}"),
            Self::Io(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for BootstrapError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Fleet(e) => Some(e),
            Self::Io(e) => Some(e),
        }
    }
}

impl From<FleetError> for BootstrapError {
    fn from(value: FleetError) -> Self {
        Self::Fleet(value)
    }
}

impl From<IoInitError> for BootstrapError {
    fn from(value: IoInitError) -> Self {
        Self::Io(value)
    }
}

/// Cancel a HostAwait token (tests / host policies).
pub fn abort_token(token: &CancellationToken) {
    token.cancel();
}
