use std::sync::Arc;

use byteflow::{FlowOutcome, HostAwaitOp, NullSink, Program, Runtime, Value};
use byteflow_io::{ByteflowIo, CancellationToken, IoConfig, IoDriver, IoDriverError};

const ECHO_AND_INCREMENT: HostAwaitOp = 1;

#[derive(Debug)]
struct DemoDriver;

impl IoDriver for DemoDriver {
    fn try_submit(&self, _command: byteflow_io::IoCommand) -> Result<u64, IoDriverError> {
        Err(IoDriverError::new("no fast device commands in this example"))
    }

    fn execute_host_await(
        &self,
        op: HostAwaitOp,
        args: Value,
        cancel: &CancellationToken,
    ) -> Result<Value, IoDriverError> {
        if cancel.is_cancelled() {
            return Err(IoDriverError::new("operation cancelled"));
        }
        if op != ECHO_AND_INCREMENT {
            return Err(IoDriverError::new(format!("unknown host operation: {op}")));
        }

        let number = args
            .as_int()
            .ok_or_else(|| IoDriverError::new("expected Int argument"))?;
        Ok(Value::Int(number + 1))
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let driver = Arc::new(DemoDriver);
    let io = ByteflowIo::new(driver, IoConfig::default())?;
    let output = Arc::new(NullSink);

    let mut program = Program::new("byteflow-io-host-await-echo");
    program.function("main", 0, |f| {
        let input = f.load_i32(41);
        let _ticket = f.host_await(ECHO_AND_INCREMENT, input);
        let reply = f.receive();
        let result = f.hop_payload(reply);
        f.return_(result);
    });

    let natives = byteflow_io::build_native_table(io.driver().clone(), output.clone())?;
    let config = io.runtime_config(output);
    let runtime = Runtime::with_natives_and_config(program.build(), natives, config)?;

    let outcome = runtime.spawn(0, &[])?.join();
    runtime.shutdown();
    io.shutdown()?;

    match outcome {
        FlowOutcome::Completed(Value::Int(42)) => {
            println!("HostAwait completed: 41 -> 42");
            Ok(())
        }
        other => Err(format!("unexpected flow outcome: {other:?}").into()),
    }
}
