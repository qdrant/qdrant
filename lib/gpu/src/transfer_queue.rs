use ash::vk;

/// What `Device::transfer_queue` is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransferQueueKind {
    /// A queue of a transfer-only queue family (a copy engine), in a different family than the
    /// compute queue. Copies on it run alongside the compute queue's work.
    Dedicated,

    /// The device has no transfer-only queue family, so the transfer queue is the compute queue
    /// itself. Copies are correct but share the queue with compute work, so they may not overlap.
    ComputeQueue,
}

/// The first queue family that supports transfers and nothing else, ignoring sparse binding and
/// protected memory. Families that also do graphics, compute, or video don't count: copies on them
/// would share an engine with that work.
pub(crate) fn dedicated_transfer_family(
    vk_queue_families: &[vk::QueueFamilyProperties],
) -> Option<usize> {
    let ignored_flags = vk::QueueFlags::SPARSE_BINDING | vk::QueueFlags::PROTECTED;
    vk_queue_families.iter().position(|vk_queue_family| {
        vk_queue_family.queue_count > 0
            && vk_queue_family.queue_flags & !ignored_flags == vk::QueueFlags::TRANSFER
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn family(queue_flags: vk::QueueFlags, queue_count: u32) -> vk::QueueFamilyProperties {
        vk::QueueFamilyProperties::default()
            .queue_flags(queue_flags)
            .queue_count(queue_count)
    }

    #[test]
    fn picks_the_transfer_only_family() {
        // Queue families of an RTX 3090 on the NVIDIA driver.
        let families = [
            family(
                vk::QueueFlags::GRAPHICS
                    | vk::QueueFlags::COMPUTE
                    | vk::QueueFlags::TRANSFER
                    | vk::QueueFlags::SPARSE_BINDING,
                16,
            ),
            family(vk::QueueFlags::TRANSFER | vk::QueueFlags::SPARSE_BINDING, 2),
            family(
                vk::QueueFlags::COMPUTE | vk::QueueFlags::TRANSFER | vk::QueueFlags::SPARSE_BINDING,
                8,
            ),
            family(
                vk::QueueFlags::TRANSFER
                    | vk::QueueFlags::SPARSE_BINDING
                    | vk::QueueFlags::VIDEO_DECODE_KHR,
                1,
            ),
        ];
        assert_eq!(dedicated_transfer_family(&families), Some(1));
    }

    #[test]
    fn skips_video_and_empty_families() {
        let families = [
            family(vk::QueueFlags::GRAPHICS | vk::QueueFlags::COMPUTE, 1),
            family(
                vk::QueueFlags::TRANSFER | vk::QueueFlags::VIDEO_ENCODE_KHR,
                1,
            ),
            family(vk::QueueFlags::TRANSFER, 0),
            family(vk::QueueFlags::TRANSFER | vk::QueueFlags::PROTECTED, 1),
        ];
        assert_eq!(dedicated_transfer_family(&families), Some(3));
    }

    #[test]
    fn none_without_a_transfer_only_family() {
        // One family for everything.
        let single = [family(
            vk::QueueFlags::GRAPHICS | vk::QueueFlags::COMPUTE | vk::QueueFlags::TRANSFER,
            1,
        )];
        assert_eq!(dedicated_transfer_family(&single), None);

        // Graphics and compute families only, transfers implied.
        let implied = [
            family(vk::QueueFlags::GRAPHICS | vk::QueueFlags::COMPUTE, 1),
            family(vk::QueueFlags::COMPUTE, 4),
        ];
        assert_eq!(dedicated_transfer_family(&implied), None);
    }
}
