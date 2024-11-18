use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use blob_store::fixtures::Payload;
use bustle::CollectionHandle;
use parking_lot::RwLock;
use serde_json::json;
use tempfile::TempDir;

#[derive(Clone)]
pub struct ArcStorage<S> {
    pub proxy: Arc<RwLock<StorageProxy<S>>>,
    pub dir: Arc<TempDir>,
}

/// A storage that includes an external to internal id tracker, and a generator of payloads.
pub struct StorageProxy<S> {
    storage: S,
    id_tracker: HashMap<u64, u32>,
    max_internal_id: u32,
    payload_picker: PayloadPicker,

    /// Amount of writes without a flush.
    write_count: u32,
}

impl<S> StorageProxy<S> {
    pub fn new(storage: S) -> Self {
        Self {
            storage,
            id_tracker: HashMap::new(),
            max_internal_id: 0,
            payload_picker: PayloadPicker::new(),
            write_count: 0,
        }
    }
}

impl<S: SequentialCollectionHandle> StorageProxy<S> {
    const FLUSH_INTERVAL: u32 = 10000;

    fn maybe_flush(&mut self) {
        if self.write_count >= Self::FLUSH_INTERVAL {
            self.write_count = 0;
            assert!(self.storage.flush());
        } else {
            self.write_count += 1;
        }
    }
}

impl<S: SequentialCollectionHandle> CollectionHandle for ArcStorage<S> {
    type Key = u64;

    fn get(&mut self, key: &Self::Key) -> bool {
        // eprintln!("GET {}", key);
        let proxy = self.proxy.read();

        let Some(internal) = proxy.id_tracker.get(key) else {
            return false;
        };

        proxy.storage.get(internal)
    }

    fn insert(&mut self, key: &Self::Key) -> bool {
        // eprintln!("INSERT {}", key);
        let mut proxy = self.proxy.write();

        proxy.maybe_flush();

        let internal_id = if let Some(id) = proxy.id_tracker.get(key) {
            *id
        } else {
            let internal_id = proxy.max_internal_id;
            proxy.max_internal_id += 1;
            proxy.id_tracker.insert(*key, internal_id);
            internal_id
        };

        let payload = proxy.payload_picker.pick(internal_id);

        proxy.storage.insert(internal_id, &payload)
    }

    fn remove(&mut self, key: &Self::Key) -> bool {
        // eprintln!("REMOVE {}", key);
        let mut proxy = self.proxy.write();

        proxy.maybe_flush();

        let internal_id = match proxy.id_tracker.get(key) {
            Some(internal_id) => *internal_id,
            None => return false,
        };

        proxy.storage.remove(&internal_id)
    }

    fn update(&mut self, key: &Self::Key) -> bool {
        // eprintln!("UPDATE {}", key);
        let mut proxy = self.proxy.write();

        proxy.maybe_flush();

        let Some(&internal_id) = proxy.id_tracker.get(key) else {
            return false;
        };
        let payload = proxy.payload_picker.pick(internal_id);
        proxy.storage.update(&internal_id, &payload)
    }
}

pub trait SequentialCollectionHandle {
    fn get(&self, key: &u32) -> bool;

    fn insert(&mut self, key: u32, payload: &Payload) -> bool;

    fn remove(&mut self, key: &u32) -> bool;

    fn update(&mut self, key: &u32, payload: &Payload) -> bool;

    fn flush(&self) -> bool;
}

pub struct PayloadPicker {
    payloads: OnceLock<Vec<Arc<Payload>>>,
}

impl PayloadPicker {
    fn new() -> Self {
        Self {
            payloads: OnceLock::new(),
        }
    }

    fn pick(&self, internal_id: u32) -> Arc<Payload> {
        let payloads = self.payloads.get_or_init(|| {
           [
                json!({"name": "Alice", "age": 30, "city": "Wonderland"}),
                json!({"name": "Bob", "age": 25, "city": "Builderland", "occupation": "Builder"}),
                json!({"name": "Charlie", "age": 35, "city": "Chocolate Factory", "hobbies": ["Inventing", "Exploring"]}),
                json!({"name": "Dave", "age": 40, "city": "Dinosaur Land", "favorite_dinosaur": "T-Rex"}),
                json!({"name": "Eve", "age": 28, "city": "Eden", "skills": ["Gardening", "Cooking", "Singing"]}),
                json!({"name": "Frank", "age": 33, "city": "Fantasy Island", "adventures": ["Treasure Hunt", "Dragon Slaying", "Rescue Mission"]}),
                json!({"name": "Grace", "age": 29, "city": "Gotham", "alias": "Batwoman", "gadgets": ["Batarang", "Grapple Gun", "Smoke Bomb"]}),
                json!({"name": "Hank", "age": 45, "city": "Hogwarts", "house": "Gryffindor", "patronus": "Stag", "wand": {"wood": "Holly", "core": "Phoenix Feather", "length": 11}}),
                json!({"name": "Ivy", "age": 27, "city": "Ivory Tower", "profession": "Scholar", "publications": ["Theories of Magic", "History of the Ancients", "Alchemy and Potions"]}),
                json!({"name": "Jack", "age": 32, "city": "Jack's Beanstalk", "adventures": ["Climbing the Beanstalk", "Meeting the Giant", "Stealing the Golden Goose", "Escaping the Giant", "Living Happily Ever After"]}),
            ].into_iter().map(|value| Arc::new(Payload(value.as_object().unwrap().clone()))).collect()
        });

        let pick_idx = internal_id as usize % payloads.len();

        payloads[pick_idx].clone()
    }
}
