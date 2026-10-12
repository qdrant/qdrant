use segment::common::operation_error::{OperationError, OperationResult};
use segment::types::SeqNumberType;
use shard::operations::OperationWithClockTag;
use shard::wal::{SerdeWal, WalRawRecord};

/// Assigns operation numbers to updates, in the order they are applied.
#[derive(Debug)]
pub(super) enum UpdateLog {
    /// Updates are written to the WAL, which numbers them, before they are applied.
    Wal(Box<SerdeWal<OperationWithClockTag>>),
    /// Updates are not persisted before they are applied. Numbering continues from the highest
    /// version found in the segments on load, so new updates are never older than stored data.
    Disabled { last_operation_id: SeqNumberType },
}

impl UpdateLog {
    /// Assign the next operation number to `record`, writing it to the WAL if there is one.
    pub fn append(&mut self, record: &OperationWithClockTag) -> OperationResult<SeqNumberType> {
        match self {
            Self::Wal(wal) => {
                let raw_record = WalRawRecord::new(record).map_err(service_error)?;
                wal.write(&raw_record).map_err(service_error)
            }
            Self::Disabled { last_operation_id } => {
                *last_operation_id += 1;
                Ok(*last_operation_id)
            }
        }
    }

    pub fn flush(&mut self) -> OperationResult<()> {
        match self {
            Self::Wal(wal) => wal
                .flush()
                .map_err(|e| OperationError::service_error(format!("WAL flush failed: {e}"))),
            Self::Disabled {
                last_operation_id: _,
            } => Ok(()),
        }
    }

    /// Drop WAL entries up to `persisted_version`, which the segments have persisted.
    pub fn ack(&mut self, persisted_version: SeqNumberType) -> OperationResult<()> {
        match self {
            Self::Wal(wal) => wal
                .ack(persisted_version)
                .map_err(|e| OperationError::service_error(format!("WAL ack failed: {e}"))),
            Self::Disabled {
                last_operation_id: _,
            } => Ok(()),
        }
    }
}

fn service_error(err: impl std::fmt::Display) -> OperationError {
    OperationError::service_error(err.to_string())
}
