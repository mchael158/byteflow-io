# Validation status

Aligned against the local workspace crate at `../Bytecode/crates/byteflow` (`byteflow-actors` **0.9.21**, lib `byteflow`).

APIs used from the core (no shims):

- `HostAwaitBridge` / `HostAwaitRequest` / `HostAwaitCompleter` / `HostAwaitOp`
- `finish_host_await` for settle/cancel races
- `RuntimeConfig::{host_await, max_host_awaits, output}`
- `Runtime::with_natives_and_config` / `Runtime::send` / `Runtime::shutdown`
- `Message::request`, `Value::Message`, `TAG_SYS_AWAIT`
- `std_native` / `std_native_table_with` / `NativeTableBuilder::register_at`
- `expect_arg` / `expect_int` / `expect_bool` / `Fault::NativeError`
- `OutputSink` / `NullSink`

Before merging into the Byteflow repository as `crates/byteflow-io`:

1. Change the path dependency to `{ path = "../byteflow" }` and add the member to the workspace.
2. Run with a toolchain that satisfies the core `rust-version` (currently `1.99.1`), or `cargo … --ignore-rust-version` on older rustc.
3. `cargo fmt --all -- --check`
4. `cargo check -p byteflow-io --all-targets`
5. `cargo test -p byteflow-io --all-targets`
6. `cargo run -p byteflow-io --example host_await_echo`

No physical MQTT broker, UART device, GPIO chip, ESP32, or payment provider is required for the default test suite. Concrete adapters belong in the host application.
