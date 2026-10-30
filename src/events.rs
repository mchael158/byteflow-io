//! Trusted host → flow event delivery via Atomic Hop messages.

use byteflow::{FlowId, Message, Runtime, SendError, Value};

/// Helper for injecting driver events into a Byteflow flow.
///
/// Always delivers [`Value::Message`]. Bare scalars violate Atomic Hop rules
/// and are rejected by [`Runtime::send`].
pub struct EventRouter<'runtime> {
    runtime: &'runtime Runtime,
}

impl<'runtime> EventRouter<'runtime> {
    /// Borrow a running runtime for trusted host-side delivery.
    pub fn new(runtime: &'runtime Runtime) -> Self {
        Self { runtime }
    }

    /// Deliver an event through the authenticated host send path.
    ///
    /// The runtime stamps host sender identity. `target` must come from
    /// trusted host bookkeeping — never from untrusted bytecode in a
    /// multi-tenant embedding.
    ///
    /// # Errors
    ///
    /// Propagates [`SendError`] from [`Runtime::send`].
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
