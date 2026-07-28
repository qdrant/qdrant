use std::path::Path;

#[cfg(target_os = "linux")]
use common::flags::feature_flags;

#[cfg(target_os = "linux")]
use crate::common::io_uring::{IoUringFallback, use_io_uring};
use crate::common::operation_error::OperationResult;
use crate::payload_storage::payload_storage_enum::PayloadStorageEnum;
use crate::payload_storage::payload_storage_impl::PayloadStorageImpl;
use crate::types::SegmentConfig;

pub(crate) fn create_payload_storage(
    segment_path: &Path,
    config: &SegmentConfig,
) -> OperationResult<PayloadStorageEnum> {
    let memory = config.payload_storage_type.memory();
    let populate = memory.populate_on_open();

    #[cfg(target_os = "linux")]
    if use_io_uring(
        IoUringFallback::Mmap,
        memory,
        feature_flags().async_payload_storage,
    ) {
        match PayloadStorageImpl::open_or_create(segment_path.to_path_buf(), populate) {
            Ok(storage) => return Ok(PayloadStorageEnum::IoUring(storage)),
            Err(err) => {
                log::error!("Failed to open io_uring based payload storage: {err}");
            }
        }
    }

    let payload_storage = PayloadStorageImpl::open_or_create(segment_path.to_path_buf(), populate)?;
    Ok(PayloadStorageEnum::Mmap(payload_storage))
}
