use byteflow::{FlowId, Message, Runtime, SendError, Value};

/// Helper for injecting driver events into a Byteflow flow.
pub struct EventRouter<'runtime> {
    runtime: &'runtime Runtime,
}

impl<'runtime> EventRouter<'runtime> {
    pub fn new(runtime: &'runtime Runtime) -> Self {
        Self { runtime }
    }

    /// Deliver an event through the trusted host path.
    ///
    /// `Runtime::send` authenticates the host as sender and enforces the
    /// Atomic Hop rule. This method never sends a bare scalar.
    pub fn send(
        &self,
        target: FlowId,
        request_id: u64,
        tag: u16,
        payload: Value,
    ) -> Result<(), SendError> {
        let message = Message::request(request_id, tag, payload);
        self.runtime.send(target, Value::Message(message))
    }
}

/// Suggested application event tags. Reserve Byteflow's system tags and keep
/// application tags stable when `.bf` modules are flashed independently.
pub mod event_tag {
    pub const COIN_INSERTED: u16 = 10;
    pub const CYCLE_COMPLETED: u16 = 11;
    pub const MACHINE_FAULT: u16 = 12;
    pub const ESP_COMMAND_ACK: u16 = 13;
}
