use std::sync::Arc;

use ahash::AHashMap;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use parking_lot::RwLock;
use rocksdb::DB;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::rocksdb_buffered_delete_wrapper::DatabaseColumnScheduledDeleteWrapper;
use crate::common::rocksdb_wrapper::{DB_PAYLOAD_CF, DatabaseColumnWrapper};
use crate::types::Payload;

/// In-memory implementation of `PayloadStorage`.
/// Persists all changes to disk using `store`, but only uses this storage during the initial load
#[derive(Debug)]
pub struct SimplePayloadStorage {
    pub(crate) payload: AHashMap<PointOffsetType, Payload>,
    pub(crate) db_wrapper: DatabaseColumnScheduledDeleteWrapper,
}

impl SimplePayloadStorage {
    pub fn open(database: Arc<RwLock<DB>>) -> OperationResult<Self> {
        let mut payload_map: AHashMap<PointOffsetType, Payload> = Default::default();

        let db_wrapper = DatabaseColumnScheduledDeleteWrapper::new(DatabaseColumnWrapper::new(
            database,
            DB_PAYLOAD_CF,
        ));

        for (key, val) in db_wrapper.lock_db().iter()? {
            let point_id: PointOffsetType = serde_cbor::from_slice(&key)
                .map_err(|_| OperationError::service_error("cannot deserialize point id"))?;
            let payload: Payload = serde_cbor::from_slice(&val)
                .map_err(|_| OperationError::service_error("cannot deserialize payload"))?;
            payload_map.insert(point_id, payload);
        }

        Ok(SimplePayloadStorage {
            payload: payload_map,
            db_wrapper,
        })
    }

    pub(crate) fn update_storage(
        &self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let point_id_serialized = serde_cbor::to_vec(&point_id).unwrap();
        hw_counter
            .payload_io_write_counter()
            .incr_delta(point_id_serialized.len());

        match self.payload.get(&point_id) {
            None => self.db_wrapper.remove(point_id_serialized),
            Some(payload) => {
                let payload_serialized = serde_cbor::to_vec(payload).unwrap();
                hw_counter
                    .payload_io_write_counter()
                    .incr_delta(payload_serialized.len());
                self.db_wrapper.put(point_id_serialized, payload_serialized)
            }
        }
    }

    pub fn payload_ptr(&self, point_id: PointOffsetType) -> Option<&Payload> {
        self.payload.get(&point_id)
    }

    /// Destroy this payload storage, remove persisted data from RocksDB
    pub fn destroy(&self) -> OperationResult<()> {
        self.db_wrapper.remove_column_family()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rand::distr::SampleString;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use rand_distr::Alphanumeric;
    use tempfile::Builder;

    use super::*;
    use crate::common::rocksdb_wrapper::open_db;
    use crate::payload_json;
    use crate::payload_storage::PayloadStorage;
    use crate::payload_storage::payload_storage_enum::PayloadStorageEnum;
    use crate::segment_constructor::migrate_rocksdb_payload_storage_to_mmap;

    const RAND_SEED: u64 = 42;

    /// Create RocksDB based payload storage.
    ///
    /// Migrate it to the mmap based payload storage and assert correctness.
    #[test]
    fn test_migrate_simple_to_mmap() {
        const POINT_COUNT: PointOffsetType = 128;
        const DELETE_PROBABILITY: f64 = 0.1;

        let mut rng = StdRng::seed_from_u64(RAND_SEED);
        let hw_counter = HardwareCounterCell::disposable();

        let db_dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let db = open_db(db_dir.path(), &[DB_PAYLOAD_CF]).unwrap();

        // Create simple payload storage, insert test data and delete some again
        let mut storage =
            PayloadStorageEnum::SimplePayloadStorage(SimplePayloadStorage::open(db).unwrap());
        for internal_id in 0..POINT_COUNT {
            let payload = payload_json! {
                "a": rng.random_range(0..100),
                "b": rng.random_bool(0.3),
                "c": Alphanumeric.sample_string(&mut rng, 8),
            };

            storage.set(internal_id, &payload, &hw_counter).unwrap();
            if rng.random_bool(DELETE_PROBABILITY) {
                storage.clear(internal_id, &hw_counter).unwrap();
            }
        }

        // Migrate from RocksDB to mmap storage
        let storage_dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let new_storage = migrate_rocksdb_payload_storage_to_mmap(&storage, storage_dir.path())
            .expect("failed to migrate from RocksDB to mmap");

        // Destroy persisted RocksDB payload data
        match storage {
            PayloadStorageEnum::SimplePayloadStorage(storage) => storage.destroy().unwrap(),
            _ => unreachable!("unexpected payload storage type"),
        }

        // We can drop RocksDB storage now
        db_dir.close().expect("failed to drop RocksDB storage");

        // Assert payload data
        let mut rng = StdRng::seed_from_u64(RAND_SEED);
        for internal_id in 0..POINT_COUNT {
            let payload = payload_json! {
                "a": rng.random_range(0..100),
                "b": rng.random_bool(0.3),
                "c": Alphanumeric.sample_string(&mut rng, 8),
            };
            let is_deleted = rng.random_bool(DELETE_PROBABILITY);

            if !is_deleted {
                assert_eq!(new_storage.get(internal_id, &hw_counter).unwrap(), payload);
            } else {
                assert_eq!(
                    new_storage
                        .get(internal_id, &hw_counter)
                        .unwrap()
                        .is_empty(),
                    is_deleted,
                );
            }
        }
    }
}
