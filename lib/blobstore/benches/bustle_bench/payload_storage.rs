use std::sync::Arc;

use blobstore::fixtures::{Payload, empty_storage};
use bustle::Collection;
use common::counter::hw;
use common::counter::hw::HwMetric;
use common::generic_consts::Random;
use common::reason::reason;
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
        let _hw = hw::unmeasured_guard(reason("No measurements needed in benches"));
        self.get_value::<Random>(*key).unwrap().is_some()
    }

    fn insert(&mut self, key: u32, payload: &Payload) -> bool {
        let _hw = hw::test_guard();
        !self
            .put_value(key, payload, HwMetric::PayloadIoWrite)
            .unwrap()
    }

    fn remove(&mut self, key: &u32) -> bool {
        self.delete_value(*key).unwrap().is_some()
    }

    fn update(&mut self, key: &u32, payload: &Payload) -> bool {
        let _hw = hw::test_guard();
        self.put_value(*key, payload, HwMetric::PayloadIoWrite)
            .unwrap()
    }

    fn flush(&self) -> bool {
        self.flusher()().is_ok()
    }
}
