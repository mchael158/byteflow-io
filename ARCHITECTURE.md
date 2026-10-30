# Architecture

## Why a separate crate

Byteflow is an embeddable concurrency runtime: bytecode execution, M:N
scheduling, mailboxes, Atomic Hop messaging, capabilities, quotas, and
supervision. It must remain free of protocol SDKs and hardware drivers.

`byteflow-io` is the host integration boundary. It supplies contracts and
bounded adapters so an application can connect MQTT, UART, GPIO, payments,
or ESP devices without enlarging the ISA or coupling the core to vendor
libraries.

| Concern | Owner |
|---------|-------|
| VM, scheduler, capabilities, mailboxes | `byteflow` |
| Bounded host queues, HostAwait workers, I/O natives | `byteflow-io` |
| Concrete brokers, ports, chips, credentials, SDKs | host application |

```text
byteflow-io  ───────────────►  byteflow
host integration                runtime / core
```

`byteflow` never depends on `byteflow-io`.

## Mechanism map

| Byteflow mechanism | Integration role |
|--------------------|------------------|
| `CallNative` / `CallNativeFrom` | Fast path: validate + enqueue only |
| `HostAwait` / `HostAwaitBridge` | Slow path: MQTT ACK, payment query, ESP wait |
| `Runtime::send` | Inbound events as `Value::Message` |
| `OutputSink` / `print` | Diagnostics only — never a control channel |
| `SendRemote` / transport | Optional; only when the peer is another process |

## Contracts

1. **No I/O opcodes.** The VM does not open sockets, files, UART, or GPIO.
2. **Fast natives are non-blocking.** `IoDriver::try_submit` validates and
   enqueues into a bounded queue; it does not wait for device or network
   completion. Success means *accepted by the host queue*.
3. **Slow work uses HostAwait.** Results return via
   `HostAwaitCompleter::complete` / `fail`, settled with
   `byteflow::finish_host_await`.
4. **Workers are bounded.** Finite thread count, finite queue, explicit
   failure on saturation. No unbounded thread-per-request fallback.
5. **Deadlines are mandatory.** Cooperative cancellation
   (`CancellationToken` / `is_cancelled`) complements timeouts; it does not
   replace them.
6. **Inbound events are hops.** `EventRouter` always wraps payloads in
   `Message::request`. Bare `Int` / `Pid` / `Str` values are rejected by
   Atomic Hop rules.
7. **Native ABI is append-only.** Slots `0..=7` are Byteflow’s standard
   table. Integration natives start at `8`.
8. **Capabilities stay in the core.** `CapRights::NATIVE` and `NativeMask`
   gate access; this crate never grants them implicitly.
9. **Fail closed.** Mutex poison, missing standard natives, and unimplemented
   HostAwait ops surface as errors — never silent success.
10. **Shutdown order.** Stop external input → `Runtime::shutdown` → drain /
    join I/O workers → driver shutdown.

## Module layout

| Module | Responsibility |
|--------|----------------|
| `lib` | `ByteflowIo`, `IoConfig`, suggested `host_op` IDs |
| `bridge` | Bounded `HostAwaitBridge` worker pool + cancellation |
| `driver` | `IoDriver` / `IoCommand` host contract |
| `queue` | `QueuedIoDriver` + `HostAwaitHandler` |
| `native_table` | Standard slots `0..=7` composed with I/O slots `8..=10` |
| `events` | `EventRouter` for trusted host → flow delivery |
| `error` | Init and native-table composition errors |

## Non-goals

- A second runtime, VM, or M:N scheduler
- Shipping concrete MQTT/UART/GPIO/payment clients in the default feature set
- Changing `.bf` format, opcodes, or core ABI to accommodate I/O
