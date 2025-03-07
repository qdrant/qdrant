use std::sync::Arc;

use bustle::Collection;
use common::counter::hardware_counter::HardwareCounterCell;
use gridstore::fixtures::{Payload, empty_storage};
use parking_lot::RwLock;

use crate::PayloadStorage;
use crate::fixture::{ArcStorage, SequentialCollectionHandle, StorageProxy};

impl Collection for ArcStorage<PayloadStorage> {
    type Handle = Self;

    fn with_capacity(_capacity: usize) -> Self {
        let (dir, storage) = empty_storage();

        let proxy = StorageProxy::new(storage);
        ArcStorage {
            proxy: Arc::new(RwLock::new(proxy)),
            dir: Arc::new(dir),
        }
    }

    fn pin(&self) -> Self::Handle {
        Self {
            proxy: self.proxy.clone(),
            dir: self.dir.clone(),
        }
    }
}

impl SequentialCollectionHandle for PayloadStorage {
    fn get(&self, key: &u32) -> bool {
        self.get_value(*key, &HardwareCounterCell::new()).is_some() // No measurements needed in benches
    }

    fn insert(&mut self, key: u32, payload: &Payload) -> bool {
        !self
            .put_value(
                key,
                payload,
                HardwareCounterCell::new().ref_payload_io_write_counter(),
            )
            .unwrap()
    }

    fn remove(&mut self, key: &u32) -> bool {
        self.delete_value(*key).is_some()
    }

    fn update(&mut self, key: &u32, payload: &Payload) -> bool {
        self.put_value(
            *key,
            payload,
            HardwareCounterCell::new().ref_payload_io_write_counter(),
        )
        .unwrap()
    }

    fn flush(&self) -> bool {
        self.flush().is_ok()
    }
}
