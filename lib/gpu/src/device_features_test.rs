use ash::vk;

use crate::*;

// A device created without half precision lists the cooperative matrix configurations of a
// normal device, without those that have f16 components.
#[test]
fn cooperative_matrices_without_half_precision() {
    let instance = crate::GPU_TEST_INSTANCE.clone();
    let physical_device = &instance.physical_devices()[0];
    let normal = Device::new(instance.clone(), physical_device).unwrap();
    let without_half = Device::new_with_params(instance.clone(), physical_device, 0, true).unwrap();
    assert!(!without_half.has_half_precision());

    let listed = |device: &Device| {
        device
            .cooperative_matrix_properties()
            .unwrap_or_default()
            .to_vec()
    };
    let has_f16 = |c: &CooperativeMatrixProperties| {
        [c.a_type, c.b_type, c.c_type, c.result_type].contains(&MatrixComponentType::Float16)
    };
    let expected: Vec<_> = listed(&normal)
        .into_iter()
        .filter(|configuration| !has_f16(configuration))
        .collect();
    assert_eq!(listed(&without_half), expected);
}

// The budget is about the largest device-local heap, and at most its size.
#[test]
fn memory_budget_is_of_the_largest_device_local_heap() {
    let instance = crate::GPU_TEST_INSTANCE.clone();
    let physical_device = &instance.physical_devices()[0];
    let device = Device::new(instance.clone(), physical_device).unwrap();
    let memory_properties = unsafe {
        instance
            .vk_instance()
            .get_physical_device_memory_properties(physical_device.vk_physical_device)
    };
    let largest_device_local = memory_properties
        .memory_heaps_as_slice()
        .iter()
        .filter(|heap| heap.flags.contains(vk::MemoryHeapFlags::DEVICE_LOCAL))
        .map(|heap| heap.size)
        .max()
        .expect("Vulkan requires a device-local heap");

    let budget = device.memory_budget();
    assert_eq!(budget.heap_size_bytes(), largest_device_local);
    assert!(budget.available_bytes() <= largest_device_local);
}
