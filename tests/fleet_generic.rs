//! Generic fleet profile — not laundry-specific.

use byteflow::Value;
use byteflow_io::fleet::{
    bootstrap, host_op, DeviceCommand, DeviceState, FleetProfile,
};
use byteflow_io::IoConfig;

#[test]
fn generic_profile_topics_and_arm_start() -> Result<(), Box<dyn std::error::Error>> {
    let (io, fleet, _) = bootstrap(FleetProfile::generic_site(), IoConfig::default())?;
    let device = fleet.device_id(3).expect("valid");
    assert_eq!(
        byteflow_io::fleet::cmd(fleet.profile(), device),
        "site/device/3/cmd"
    );

    let cancel = byteflow_io::CancellationToken::default();
    let driver = io.driver().clone();
    let units = driver.execute_host_await(
        host_op::APPLY_CREDIT,
        DeviceCommand::credit_args(device, 100),
        &cancel,
    )?;
    assert_eq!(units, Value::Int(100));
    assert_eq!(fleet.lookup(device)?.state, DeviceState::Armed);

    let started = driver.execute_host_await(
        host_op::START_ACTION_AND_WAIT,
        DeviceCommand::start_args(device),
        &cancel,
    )?;
    assert_eq!(started, Value::Int(3));
    assert_eq!(fleet.lookup(device)?.state, DeviceState::Busy);
    io.shutdown()?;
    Ok(())
}

#[test]
fn custom_small_fleet_capacity() -> Result<(), Box<dyn std::error::Error>> {
    let profile = FleetProfile {
        max_devices: 4,
        topic_root: "lab/node".to_owned(),
        label: "node".to_owned(),
    };
    let (io, fleet, _) = bootstrap(profile, IoConfig::default())?;
    assert!(fleet.device_id(0).is_none());
    assert!(fleet.device_id(4).is_some());
    assert!(fleet.device_id(5).is_none());
    assert_eq!(fleet.all_ids().count(), 4);
    io.shutdown()?;
    Ok(())
}
