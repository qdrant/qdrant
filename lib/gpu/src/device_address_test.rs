use std::sync::Arc;
use std::time::Duration;

use crate::*;

// A buffer handed to `Context::keep_alive` outlives the caller's handle until the context's work
// has finished, and is released then. A shader reaches a buffer by its device address without a
// descriptor set that holds it, so the context has to be told.
#[test]
fn context_keeps_buffer_alive() {
    let instance = crate::GPU_TEST_INSTANCE.clone();
    let device = Device::new(instance.clone(), &instance.physical_devices()[0]).unwrap();
    let options = BufferOptions {
        queues: BufferQueues::Compute,
        device_address: match device.has_buffer_device_address() {
            true => DeviceAddress::Enabled,
            false => DeviceAddress::Disabled,
        },
    };
    let kept = Buffer::new_with_options(device.clone(), "Kept", BufferType::Storage, 1024, options)
        .unwrap();
    assert_eq!(
        kept.device_address().is_some(),
        device.has_buffer_device_address()
    );

    let mut context = Context::new(device.clone()).unwrap();
    let other = Buffer::new(device.clone(), "Other", BufferType::Storage, 1024).unwrap();
    context.clear_buffer(other).unwrap();
    context.keep_alive(std::slice::from_ref(&kept)).unwrap();
    context.run().unwrap();

    let weak = Arc::downgrade(&kept);
    drop(kept);
    assert!(weak.upgrade().is_some(), "held until the work has finished");
    context.wait_finish(Duration::from_secs(60)).unwrap();
    assert!(
        weak.upgrade().is_none(),
        "released once the work has finished"
    );
}
