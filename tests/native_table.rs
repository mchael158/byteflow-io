use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use byteflow::{NullSink, Value};
use byteflow_io::{build_native_table, native_index, IoCommand, IoDriver, IoDriverError};

#[derive(Debug, Default)]
struct TestDriver {
    next_id: AtomicU64,
    commands: Mutex<Vec<IoCommand>>,
}

impl IoDriver for TestDriver {
    fn try_submit(&self, command: IoCommand) -> Result<u64, IoDriverError> {
        match self.commands.lock() {
            Ok(mut commands) => commands.push(command),
            Err(_) => return Err(IoDriverError::new("test command queue poisoned")),
        }
        Ok(self.next_id.fetch_add(1, Ordering::Relaxed) + 1)
    }
}

#[test]
fn standard_native_slots_are_preserved_and_io_slots_are_appended() {
    let driver = Arc::new(TestDriver::default());
    let output = Arc::new(NullSink);
    let table = build_native_table(driver, output).expect("native table should build");

    assert_eq!(table.index_of("print"), Some(native_index::PRINT));
    assert_eq!(table.index_of("now_ms"), Some(native_index::NOW_MS));
    assert_eq!(table.index_of("make_msg"), Some(native_index::MAKE_MSG));
    assert_eq!(table.index_of("msg_sender"), Some(native_index::MSG_SENDER));
    assert_eq!(table.index_of("msg_request_id"), Some(native_index::MSG_REQUEST_ID));
    assert_eq!(table.index_of("msg_tag"), Some(native_index::MSG_TAG));
    assert_eq!(table.index_of("msg_payload"), Some(native_index::MSG_PAYLOAD));
    assert_eq!(table.index_of("msg_reply_cap"), Some(native_index::MSG_REPLY_CAP));

    assert_eq!(table.index_of("io_gpio_write"), Some(8));
    assert_eq!(table.index_of("io_mqtt_publish_fast"), Some(9));
    assert_eq!(table.index_of("io_uart_write_fast"), Some(10));
    assert_eq!(table.len(), 11);
}

#[test]
fn gpio_native_only_enqueues_and_returns_a_command_id() {
    let driver = Arc::new(TestDriver::default());
    let output = Arc::new(NullSink);
    let table = build_native_table(driver.clone(), output).expect("native table should build");
    let gpio = table
        .get(native_index::GPIO_WRITE)
        .expect("GPIO native must be registered");

    let result = gpio(&[Value::Int(17), Value::Bool(true)]);
    assert!(matches!(result, Ok(Value::Int(1))));

    let commands = driver.commands.lock().expect("test mutex");
    assert_eq!(
        commands.as_slice(),
        &[IoCommand::GpioWrite {
            pin: 17,
            high: true
        }]
    );
}

#[test]
fn mqtt_native_rejects_invalid_qos_before_touching_driver() {
    let driver = Arc::new(TestDriver::default());
    let output = Arc::new(NullSink);
    let table = build_native_table(driver.clone(), output).expect("native table should build");
    let mqtt = table
        .get(native_index::MQTT_PUBLISH_FAST)
        .expect("MQTT native must be registered");

    let result = mqtt(&[
        Value::str("devices/washer-01/cmd"),
        Value::Int(1),
        Value::Int(3),
    ]);
    assert!(result.is_err());

    let commands = driver.commands.lock().expect("test mutex");
    assert!(commands.is_empty());
}

#[derive(Debug)]
struct NoopAwaitHandler;

impl byteflow_io::HostAwaitHandler for NoopAwaitHandler {
    fn execute(
        &self,
        _op: u32,
        _args: Value,
        _cancel: &byteflow_io::CancellationToken,
    ) -> Result<Value, IoDriverError> {
        Ok(Value::Unit)
    }
}

#[test]
fn queued_driver_is_bounded_and_returns_before_device_execution() {
    use byteflow_io::{IoCommandReceiver, QueuedIoDriver};
    use std::sync::Arc;

    let handler = Arc::new(NoopAwaitHandler);
    let (driver, receiver): (Arc<QueuedIoDriver>, IoCommandReceiver) =
        QueuedIoDriver::new(1, handler).expect("bounded driver should build");

    let first = driver
        .try_submit(IoCommand::GpioWrite {
            pin: 4,
            high: true,
        })
        .expect("first command should fit");
    assert_eq!(first, 1);

    let second = driver.try_submit(IoCommand::GpioWrite {
        pin: 5,
        high: false,
    });
    assert!(second.is_err(), "second command must not wait for queue space");

    let queued = receiver.try_recv().expect("first command should be queued");
    assert_eq!(queued.id, first);
    assert_eq!(
        queued.command,
        IoCommand::GpioWrite {
            pin: 4,
            high: true
        }
    );
}

#[derive(Debug)]
struct EchoAwaitDriver;

impl IoDriver for EchoAwaitDriver {
    fn try_submit(&self, _command: IoCommand) -> Result<u64, IoDriverError> {
        Err(IoDriverError::new("fast commands are not used by this test"))
    }

    fn execute_host_await(
        &self,
        op: u32,
        args: Value,
        cancel: &byteflow_io::CancellationToken,
    ) -> Result<Value, IoDriverError> {
        if cancel.is_cancelled() {
            return Err(IoDriverError::new("cancelled"));
        }
        if op != 77 {
            return Err(IoDriverError::new("unexpected test op"));
        }
        Ok(args)
    }
}

#[test]
fn host_await_bridge_completes_a_waiting_flow() -> Result<(), Box<dyn std::error::Error>> {
    use byteflow::{FlowOutcome, Program, Runtime, RuntimeConfig, TAG_SYS_AWAIT};
    use byteflow_io::IoConfig;

    let driver = Arc::new(EchoAwaitDriver);
    let io = byteflow_io::ByteflowIo::new(driver, IoConfig::default())?;
    let bridge = io.host_await_bridge();

    let mut program = Program::new("host-await-integration-test");
    program.function("main", 0, |f| {
        let input = f.load_i32(42);
        let _ticket = f.host_await(77, input);
        let message = f.receive();
        f.return_(message);
    });

    let config = RuntimeConfig {
        host_await: Some(bridge),
        ..RuntimeConfig::default()
    };
    let runtime = Runtime::with_config(program.build(), config)?;
    let outcome = runtime.spawn(0, &[])?.join();
    runtime.shutdown();
    io.shutdown()?;

    assert!(matches!(
        outcome,
        FlowOutcome::Completed(Value::Message(ref message))
            if message.tag == TAG_SYS_AWAIT
                && matches!(message.payload.as_ref(), Value::Int(42))
    ));
    Ok(())
}
