use crate::types::{Payload, PointOffsetType};
use std::collections::HashMap;
use std::path::Path;

use rocksdb::{IteratorMode, Options, DB};

use crate::entry::entry_point::OperationResult;

const DB_CACHE_SIZE: usize = 10 * 1024 * 1024; // 10 mb
const DB_NAME: &str = "payload";

/// In-memory implementation of `PayloadStorage`.
/// Persists all changes to disk using `store`, but only uses this storage during the initial load
pub struct SimplePayloadStorage {
    pub(crate) payload: HashMap<PointOffsetType, Payload>,
    pub(crate) store: DB,
}

impl SimplePayloadStorage {
    pub fn open(path: &Path) -> OperationResult<Self> {
        if !path.exists() {
            std::fs::create_dir(path)?;
        }
        if !path.join("CURRENT").exists() {
            for entry in std::fs::read_dir(Path::new("./empty_dbs/payload_storage")).unwrap() {
                let src_path = entry.unwrap().path();
                if !src_path.is_dir() {
                    match src_path.file_name() {
                        Some(filename) => {
                            let dest_path = path.join(filename);
                            std::fs::copy(&src_path, &dest_path)?;
                        }
                        None => {}
                    }
                }
            }
        }

        let mut options: Options = Options::default();
        options.set_write_buffer_size(DB_CACHE_SIZE);
        options.create_if_missing(false);
        options.create_missing_column_families(true);
        let store = DB::open_cf(&options, path, [DB_NAME])?;

        let mut payload_map: HashMap<PointOffsetType, Payload> = Default::default();

        let cf_handle = store.cf_handle(DB_NAME).unwrap();
        for (key, val) in store.iterator_cf(cf_handle, IteratorMode::Start) {
            let point_id: PointOffsetType = serde_cbor::from_slice(&key).unwrap();
            let payload: Payload = serde_cbor::from_slice(&val).unwrap();
            payload_map.insert(point_id, payload);
        }

        Ok(SimplePayloadStorage {
            payload: payload_map,
            store,
        })
    }

    pub(crate) fn update_storage(&self, point_id: &PointOffsetType) -> OperationResult<()> {
        let cf_handle = self.store.cf_handle(DB_NAME).unwrap();
        match self.payload.get(point_id) {
            None => self
                .store
                .delete_cf(cf_handle, serde_cbor::to_vec(&point_id).unwrap())?,
            Some(payload) => self.store.put_cf(
                cf_handle,
                serde_cbor::to_vec(&point_id).unwrap(),
                serde_cbor::to_vec(payload).unwrap(),
            )?,
        };
        Ok(())
    }

    pub fn payload_ptr(&self, point_id: PointOffsetType) -> Option<&Payload> {
        self.payload.get(&point_id)
    }
}
