# byteflow-io

Host-side I/O integration layer for `byteflow-actors` (Rust import: `byteflow`).

This crate intentionally does **not** add I/O opcodes, sockets, GPIO, MQTT, UART,
or payment clients to the Byteflow VM. The host application owns those systems.

## Responsibilities

- Fast `CallNative` adapters: validate arguments and enqueue bounded commands only.
- `HostAwaitBridge`: execute slow work on a separate, bounded host worker pool.
- `Runtime::send` event helper: inject external events as `Value::Message`.
- Standard native table composition: preserve Byteflow slots `0..=7` and append I/O slots `8..=10`.

## Compatibility

Aligned with **`byteflow-actors` 0.9.21+** (Rust import: `byteflow`). This standalone repo depends on the core via git:

```toml
byteflow-actors = { git = "https://github.com/mchael158/byteflow-actors.git" }
```

When the crate lives inside the Byteflow workspace as `crates/byteflow-io`, switch to `{ path = "../byteflow" }`. Do not add shim APIs or change the core ABI to make this integration crate compile.

## Install in a Byteflow workspace

Add `crates/byteflow-io` as a workspace member, then depend on it from the host binary:

```toml
[dependencies]
byteflow-actors = { path = "../byteflow" }
byteflow-io = { path = "../byteflow-io" }
```

## Ready-to-use bounded command queue

`QueuedIoDriver` implements the `IoDriver` fast path with `std::sync::mpsc::sync_channel`. The host receives `QueuedCommand { id, command }` and executes the concrete hardware/broker operation on its own driver thread. The queue is bounded; a full queue returns an error immediately and the native surfaces it as a Byteflow `Fault`.

The host also implements `HostAwaitHandler` for slow request/response operations such as payment status or MQTT ACK. This keeps provider SDKs out of the core integration crate.

The command consumer is host-owned. Prefer `recv_timeout` (or `try_recv`) with the host application's own stop signal instead of an uninterruptible `recv()` loop, so shutdown can stop and join the device thread deterministically. `QueuedIoDriver::shutdown` rejects new submissions; it does not own or join the application's MQTT/UART/GPIO consumer thread.

## Fast native functions

| Index | Name | Arguments | Result |
|---:|---|---|---|
| 8 | `io_gpio_write` | `(pin: Int, high: Bool/Int)` | host command ID |
| 9 | `io_mqtt_publish_fast` | `(topic: Str, payload: Value, qos: Int)` | host command ID |
| 10 | `io_uart_write_fast` | `(port: Str, bytes: Bytes)` | host command ID |

Every native calls only `IoDriver::try_submit`. The driver must put the command into its own bounded queue and return immediately. The returned ID means “accepted by the host queue”, not “device operation completed”.

## Slow HostAwait operations

Suggested host-owned operation IDs are exposed in `host_op`:

- `MQTT_PUBLISH_AND_WAIT_ACK = 1`
- `PAYMENT_STATUS = 2`
- `START_MACHINE_CYCLE_AND_WAIT = 3`
- `ESP_OTA_CHUNK = 4`

These values are application protocol IDs, **not bytecode opcodes**. Implement `IoDriver::execute_host_await` in the host driver. It runs on a dedicated `byteflow-io` thread, not a Byteflow worker. Use finite deadlines and poll `CancellationToken` between blocking steps.

## Inbound events

```rust
use byteflow::{Runtime, Value};
use byteflow_io::{event_tag, EventRouter};

// `target_flow` is obtained from trusted host-side runtime bookkeeping.
let events = EventRouter::new(&runtime);
events.send(
    target_flow,
    1001,
    event_tag::COIN_INSERTED,
    Value::Int(500), // e.g. 500 cents
)?;
```

`EventRouter` wraps the payload in `Message::request` and calls `Runtime::send`; it never sends a bare scalar. The runtime stamps the host sender identity.

## Startup and shutdown

```rust
use std::sync::Arc;
use byteflow::{NullSink, Runtime};
use byteflow_io::{build_native_table, ByteflowIo, IoConfig};

let output = Arc::new(NullSink);
let io = ByteflowIo::new(driver, IoConfig::default())?;
let natives = build_native_table(io.driver().clone(), output.clone())?;
let config = io.runtime_config(output);
let runtime = Runtime::with_natives_and_config(chunk, natives, config)?;

// Run host application and device drivers...

// 1. Stop external callbacks and signal host-owned device-consumer loops to stop.
//    Use recv_timeout/try_recv so the loop can observe its stop signal.
// 2. Shut down Byteflow; this cancels outstanding HostAwait tickets.
runtime.shutdown();
// 3. Join host-owned device-consumer threads, then drain/join HostAwait workers
//    and call the driver's shutdown hook.
io.shutdown()?;
```

The driver must implement bounded I/O, deadlines, and cooperative cancellation. A driver that ignores cancellation or performs an unbounded blocking operation can delay `io.shutdown()`.

## Tests and example

```sh
cargo test --all-targets
cargo run --example host_await_echo
```

The test driver is deliberately an in-memory fake. Concrete broker, serial, GPIO, and payment adapters belong in the host application or optional adapter crates because they depend on the selected hardware, broker, OS, credentials, and payment provider.
