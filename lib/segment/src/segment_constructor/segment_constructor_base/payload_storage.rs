use std::path::Path;

#[cfg(target_os = "linux")]
use common::flags::feature_flags;

use crate::common::operation_error::OperationResult;
use crate::payload_storage::payload_storage_enum::PayloadStorageEnum;
use crate::payload_storage::payload_storage_impl::PayloadStorageImpl;
use crate::types::{PayloadStorageType, SegmentConfig};
#[cfg(target_os = "linux")]
use crate::vector_storage::common::get_async_scorer;

pub(crate) fn create_payload_storage(
    segment_path: &Path,
    config: &SegmentConfig,
) -> OperationResult<PayloadStorageEnum> {
    create_payload_storage_at(segment_path, config)
}

pub(crate) fn create_payload_storage_at(
    storage_path: &Path,
    config: &SegmentConfig,
) -> OperationResult<PayloadStorageEnum> {
    #[cfg(target_os = "linux")]
    match config.payload_storage_type {
        PayloadStorageType::Mmap if get_async_scorer() && feature_flags().async_payload_storage => {
            let storage = PayloadStorageImpl::open_or_create(storage_path.to_path_buf(), false);

            match storage {
                Ok(storage) => return Ok(PayloadStorageEnum::IoUring(storage)),
                Err(err) => {
                    log::error!("Failed to open io_uring based payload storage: {err}");
                }
            }
        }

        PayloadStorageType::Mmap => (),
        PayloadStorageType::InRamMmap => (),
    }

    let populate = match config.payload_storage_type {
        PayloadStorageType::Mmap => false,
        PayloadStorageType::InRamMmap => true,
    };

    let payload_storage = PayloadStorageImpl::open_or_create(storage_path.to_path_buf(), populate)?;
    Ok(PayloadStorageEnum::Mmap(payload_storage))
}
