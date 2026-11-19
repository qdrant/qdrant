use ash::vk;

use crate::*;

/// How much device-local memory (VRAM) this process can use, in bytes, from `Device::memory_budget`.
/// It's about the device's largest device-local memory heap, where `MemoryLocation::GpuOnly`
/// buffers live on a discrete GPU. The variant says where the numbers come from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemoryBudget {
    /// Reported by the driver through `VK_EXT_memory_budget`, at the time of the query.
    Reported {
        /// Size of the heap in bytes.
        heap_size_bytes: u64,
        /// How many bytes of the heap this process can allocate in total, including what it
        /// already uses, before allocations may fail or slow down. Never above the heap size.
        /// Other processes' allocations lower it.
        budget_bytes: u64,
        /// How many bytes of the heap this process uses now.
        usage_bytes: u64,
    },

    /// The driver has no `VK_EXT_memory_budget`: only the size of the heap is known, which this
    /// process shares with every other process on the device.
    HeapSize {
        /// Size of the heap in bytes.
        heap_size_bytes: u64,
    },
}

impl MemoryBudget {
    /// Size of the heap in bytes.
    pub fn heap_size_bytes(&self) -> u64 {
        match *self {
            MemoryBudget::Reported {
                heap_size_bytes,
                budget_bytes: _,
                usage_bytes: _,
            } => heap_size_bytes,
            MemoryBudget::HeapSize { heap_size_bytes } => heap_size_bytes,
        }
    }

    /// Bytes this process can still allocate: the budget minus the usage when the driver reports
    /// them, else the heap size.
    pub fn available_bytes(&self) -> u64 {
        match *self {
            MemoryBudget::Reported {
                heap_size_bytes: _,
                budget_bytes,
                usage_bytes,
            } => budget_bytes.saturating_sub(usage_bytes),
            MemoryBudget::HeapSize { heap_size_bytes } => heap_size_bytes,
        }
    }
}

/// The heap `MemoryBudget` is about: the largest heap with `DEVICE_LOCAL`. A device must have one
/// such heap, but if a driver reports none, it's the largest heap.
/// Discrete GPUs can have more than one device-local heap, for example a small host-visible
/// one (BAR) next to the VRAM.
pub(crate) fn device_local_heap_index(
    memory_properties: &vk::PhysicalDeviceMemoryProperties,
) -> usize {
    let heaps = memory_properties.memory_heaps_as_slice();
    let largest_of = |filter: fn(&vk::MemoryHeap) -> bool| {
        heaps
            .iter()
            .enumerate()
            .filter(|(_, heap)| filter(heap))
            .max_by_key(|(_, heap)| heap.size)
            .map(|(index, _)| index)
    };
    largest_of(|heap| heap.flags.contains(vk::MemoryHeapFlags::DEVICE_LOCAL))
        .or_else(|| largest_of(|_| true))
        .unwrap_or(0)
}

/// Whether the device supports `VK_EXT_memory_budget`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MemoryBudgetSupport {
    /// `VK_EXT_memory_budget` is supported and enabled.
    Extension,
    /// Only the heap sizes are known.
    HeapSizeOnly,
}

/// Query the memory budget of heap `heap_index` of `vk_physical_device`.
pub(crate) fn query_memory_budget(
    instance: &Instance,
    vk_physical_device: vk::PhysicalDevice,
    heap_index: usize,
    support: MemoryBudgetSupport,
) -> MemoryBudget {
    let mut budget_properties = vk::PhysicalDeviceMemoryBudgetPropertiesEXT::default();
    let mut memory_properties = vk::PhysicalDeviceMemoryProperties2::default();
    match support {
        MemoryBudgetSupport::Extension => {
            memory_properties = memory_properties.push_next(&mut budget_properties);
        }
        MemoryBudgetSupport::HeapSizeOnly => {}
    }
    unsafe {
        instance
            .vk_instance()
            .get_physical_device_memory_properties2(vk_physical_device, &mut memory_properties);
    }
    let heap_size_bytes = memory_properties.memory_properties.memory_heaps[heap_index].size;

    match support {
        MemoryBudgetSupport::Extension => MemoryBudget::Reported {
            heap_size_bytes,
            budget_bytes: budget_properties.heap_budget[heap_index],
            usage_bytes: budget_properties.heap_usage[heap_index],
        },
        MemoryBudgetSupport::HeapSizeOnly => MemoryBudget::HeapSize { heap_size_bytes },
    }
}
