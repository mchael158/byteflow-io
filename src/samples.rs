//! Non-normative application protocol constants for demos and host scaffolding.
//!
//! These values are **not** part of the Byteflow ISA and are not required by
//! the integration contracts. Prefer [`crate::fleet::host_op`] /
//! [`crate::fleet::event_tag`] (or the thin [`crate::laundry`] profile); this
//! module remains the shared numeric source those aliases re-export.

/// Example [`byteflow::HostAwaitOp`] discriminators for slow host work.
pub mod host_op {
    /// Publish to MQTT and wait for broker acknowledgement.
    pub const MQTT_PUBLISH_AND_WAIT_ACK: u32 = 1;
    /// Query a payment provider for transaction status.
    pub const PAYMENT_STATUS: u32 = 2;
    /// Start a machine cycle and wait for completion.
    pub const START_MACHINE_CYCLE_AND_WAIT: u32 = 3;
    /// Transfer one ESP OTA firmware chunk and wait for acknowledgement.
    pub const ESP_OTA_CHUNK: u32 = 4;
}

/// Example application event tags for inbound [`crate::EventRouter`] hops.
///
/// Keep values stable when `.bf` modules are flashed independently of the
/// host. Avoid colliding with Byteflow system tags (`TAG_SYS_*`).
pub mod event_tag {
    /// Coin or credit accepted by the payment front-end.
    pub const COIN_INSERTED: u16 = 10;
    /// Machine cycle finished successfully.
    pub const CYCLE_COMPLETED: u16 = 11;
    /// Machine reported a fault condition.
    pub const MACHINE_FAULT: u16 = 12;
    /// ESP acknowledged a previously issued command.
    pub const ESP_COMMAND_ACK: u16 = 13;
}
