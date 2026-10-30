# byteflow-io

Host-side I/O integration for [`byteflow-actors`](https://github.com/mchael158/byteflow-actors)
(Rust import: `byteflow`).

This crate keeps device and protocol concerns **outside** the Byteflow VM.
It does not add I/O opcodes, open sockets, or embed MQTT, UART, GPIO, or
payment SDKs. The host application owns those systems and plugs them in
through the contracts below.

```text
bytecode / CallNative / HostAwait
              │
              ▼
         byteflow-io          ← bounded queues, natives 8..=10, HostAwait workers
              │
              ▼
      host IoDriver           ← MQTT, UART, GPIO, payments, ESP, …
```

## What it provides

| Surface | Role |
|---------|------|
| Fast `CallNative` adapters | Validate arguments and enqueue into a bounded host queue |
| [`WorkerHostAwaitBridge`](src/bridge.rs) | Run slow work on a dedicated, bounded host worker pool |
| [`EventRouter`](src/events.rs) | Inject external events as authenticated `Value::Message` hops |
| [`build_native_table`](src/native_table.rs) | Preserve Byteflow slots `0..=7` and append I/O slots `8..=10` |
| [`QueuedIoDriver`](src/queue.rs) | Ready-made bounded command queue for the fast path |

Dependency direction is one-way: `byteflow-io` → `byteflow`. The core never
depends on this crate.

## Compatibility

Requires **`byteflow-actors` 0.9.21+** (lib name `byteflow`).

```toml
[dependencies]
byteflow-actors = { git = "https://github.com/mchael158/byteflow-actors.git" }
byteflow-io = { git = "https://github.com/mchael158/byteflow-io.git" }
```

Inside a Byteflow workspace:

```toml
[dependencies]
byteflow-actors = { path = "../byteflow" }
byteflow-io = { path = "../byteflow-io" }
```

## Quick start

```rust
use std::sync::Arc;
use byteflow::{NullSink, Runtime};
use byteflow_io::{build_native_table, ByteflowIo, IoConfig};

let output = Arc::new(NullSink);
let io = ByteflowIo::new(driver, IoConfig::default())?;
let natives = build_native_table(io.driver().clone(), output.clone())?;
let config = io.runtime_config(output);
let runtime = Runtime::with_natives_and_config(chunk, natives, config)?;

// … host application …

runtime.shutdown();
io.shutdown()?;
```

Shutdown order matters:

1. Stop external callbacks and host-owned device consumer loops
   (`recv_timeout` / `try_recv` plus an application stop signal).
2. Call `Runtime::shutdown()` — cancels outstanding HostAwait tickets.
3. Join device threads, then call `ByteflowIo::shutdown()`.

## Fast natives

| Index | Name | Arguments | Result |
|------:|------|-----------|--------|
| 8 | `io_gpio_write` | `(pin: Int, high: Bool/Int)` | host command ID |
| 9 | `io_mqtt_publish_fast` | `(topic: Str, payload: Value, qos: Int)` | host command ID |
| 10 | `io_uart_write_fast` | `(port: Str, bytes: Bytes)` | host command ID |

Each native calls only [`IoDriver::try_submit`](src/driver.rs). The driver must
enqueue and return immediately. The returned ID means *accepted by the host
queue*, not *device operation completed*.

## HostAwait operations

Suggested application operation IDs live in [`host_op`](src/lib.rs):

| Constant | Value |
|----------|------:|
| `MQTT_PUBLISH_AND_WAIT_ACK` | 1 |
| `PAYMENT_STATUS` | 2 |
| `START_MACHINE_CYCLE_AND_WAIT` | 3 |
| `ESP_OTA_CHUNK` | 4 |

These are host protocol discriminators, **not** ISA opcodes. Implement
[`IoDriver::execute_host_await`](src/driver.rs) (or
[`HostAwaitHandler`](src/queue.rs)). Work runs on a `byteflow-io` worker
thread — never on a Byteflow scheduler worker. Apply finite deadlines and
poll [`CancellationToken`](src/bridge.rs) between blocking steps.

## Inbound events

```rust
use byteflow::{Runtime, Value};
use byteflow_io::{event_tag, EventRouter};

// `target_flow` comes from trusted host-side bookkeeping, not untrusted bytecode.
let events = EventRouter::new(&runtime);
events.send(
    target_flow,
    1001,
    event_tag::COIN_INSERTED,
    Value::Int(500),
)?;
```

`EventRouter` wraps the payload in `Message::request` and calls
`Runtime::send`. Bare scalars are never delivered on the wire path.

## Queued driver

[`QueuedIoDriver`](src/queue.rs) implements the fast path with a bounded
`sync_channel`. The host drains [`IoCommandReceiver`](src/queue.rs) on its
own device thread. A full queue fails immediately; the native surfaces that
as a Byteflow `Fault`.

`QueuedIoDriver::shutdown` rejects new submissions. It does not join the
application’s MQTT/UART/GPIO consumer thread — that remains host-owned.

## Design constraints

- No new I/O opcodes; the VM never opens sockets, files, UART, or GPIO.
- `CallNative` must not wait on network, brokers, devices, or long locks.
- HostAwait workers are bounded; queue saturation fails closed.
- Completions use Byteflow’s `finish_host_await` settle path.
- Native slots `0..=7` are append-only ABI; I/O natives start at `8`.
- Access remains gated by `CapRights::NATIVE` and `NativeMask` in the core.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full contract map.

## Development

```sh
cargo test --all-targets
cargo run --example host_await_echo
```

Default tests use in-memory fakes. Hardware-in-the-loop adapters belong in
the host application or optional feature crates.

## License

Licensed under either of

- Apache License, Version 2.0, or
- MIT license

at your option.
