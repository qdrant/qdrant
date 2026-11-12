use std::sync::Arc;
use std::time::Duration;

use ash::vk;

use crate::*;

const TIMEOUT: Duration = Duration::from_secs(60);

const SHARED: BufferOptions = BufferOptions {
    queues: BufferQueues::ComputeAndTransfer,
    device_address: DeviceAddress::Disabled,
};

fn test_device() -> Arc<Device> {
    let instance = crate::GPU_TEST_INSTANCE.clone();
    let physical_device = &instance.physical_devices()[0];
    Device::new(instance.clone(), physical_device).unwrap()
}

fn shared(device: &Arc<Device>, buffer_type: BufferType, size: usize) -> Arc<Buffer> {
    Buffer::new_with_options(device.clone(), "Shared", buffer_type, size, SHARED).unwrap()
}

// A dedicated transfer queue is of another family than the compute queue, and a context submits
// to the queue it was created for.
#[test]
fn contexts_submit_to_their_queues() {
    let device = test_device();
    let (compute, transfer) = (device.compute_queue(), device.transfer_queue());
    match device.transfer_queue_kind() {
        TransferQueueKind::Dedicated => assert_ne!(
            transfer.vk_queue_family_index,
            compute.vk_queue_family_index
        ),
        TransferQueueKind::ComputeQueue => assert_eq!(transfer.vk_queue, compute.vk_queue),
    }
    let compute_context = Context::new(device.clone()).unwrap();
    let transfer_context = Context::with_queue(device.clone(), ContextQueue::Transfer).unwrap();
    assert_eq!(compute_context.vk_queue(), compute.vk_queue);
    assert_eq!(transfer_context.vk_queue(), transfer.vk_queue);
}

// A buffer for both queues is shared by the two families when they differ, and exclusive
// otherwise; a compute-only buffer is always exclusive.
#[test]
fn buffer_sharing_mode_matches_queue_families() {
    let device = test_device();
    let compute_only =
        Buffer::new(device.clone(), "Compute only", BufferType::Storage, 1024).unwrap();
    let shared = shared(&device, BufferType::Storage, 1024);

    assert_eq!(compute_only.queues(), BufferQueues::Compute);
    assert_eq!(compute_only.vk_sharing_mode(), vk::SharingMode::EXCLUSIVE);
    assert_eq!(shared.queues(), BufferQueues::ComputeAndTransfer);
    let expected = match device.transfer_queue_kind() {
        TransferQueueKind::Dedicated => vk::SharingMode::CONCURRENT,
        TransferQueueKind::ComputeQueue => vk::SharingMode::EXCLUSIVE,
    };
    assert_eq!(shared.vk_sharing_mode(), expected);
}

// A transfer context copies and clears shared buffers, and refuses everything else.
#[test]
fn transfer_context_copies_shared_buffers_only() {
    let device = test_device();
    let mut context = Context::with_queue(device.clone(), ContextQueue::Transfer).unwrap();
    let data: Vec<u32> = (0..1024).collect();
    let size = data.len() * size_of::<u32>();
    let compute_only =
        Buffer::new(device.clone(), "Compute only", BufferType::Storage, size).unwrap();
    let staging = shared(&device, BufferType::CpuToGpu, size);
    let storage = shared(&device, BufferType::Storage, size);
    let download = shared(&device, BufferType::GpuToCpu, size);

    // Buffers not created for the transfer queue, shaders, and barriers
    let refused = |result: GpuResult<()>| matches!(result, Err(GpuError::NotSupported(_)));
    assert!(refused(context.copy_gpu_buffer(
        compute_only.clone(),
        storage.clone(),
        0,
        0,
        size
    )));
    assert!(refused(context.copy_gpu_buffer(
        storage.clone(),
        compute_only.clone(),
        0,
        0,
        size
    )));
    assert!(refused(context.clear_buffer(compute_only)));
    assert!(refused(context.dispatch(1, 1, 1)));
    assert!(refused(
        context.barrier_buffers(std::slice::from_ref(&storage))
    ));

    // Each copy in a submission of its own: the second reads what the first wrote
    staging.upload(data.as_slice(), 0).unwrap();
    for (source, target) in [(&staging, &storage), (&storage, &download)] {
        context
            .copy_gpu_buffer(source.clone(), target.clone(), 0, 0, size)
            .unwrap();
        context.run().unwrap();
        context.wait_finish(TIMEOUT).unwrap();
    }
    assert_eq!(download.download_vec::<u32>(0, data.len()).unwrap(), data);
}

// Contexts on the same queue submit from several threads at once. Vulkan requires external
// synchronization of a queue in `vkQueueSubmit`, and the validation layer (on in all tests)
// reports a threading error if two threads submit to one queue at the same time.
#[test]
fn concurrent_submits_from_threads() {
    let device = test_device();
    let rounds = 3000;
    std::thread::scope(|scope| {
        for queue in [
            ContextQueue::Compute,
            ContextQueue::Compute,
            ContextQueue::Transfer,
            ContextQueue::Transfer,
        ] {
            let device = device.clone();
            scope.spawn(move || {
                let mut context = Context::with_queue(device.clone(), queue).unwrap();
                let source = shared(&device, BufferType::Storage, 256);
                let target = shared(&device, BufferType::Storage, 256);
                for _ in 0..rounds {
                    context
                        .copy_gpu_buffer(source.clone(), target.clone(), 0, 0, 256)
                        .unwrap();
                    context.run().unwrap();
                    context.wait_finish(TIMEOUT).unwrap();
                }
            });
        }
    });
}
