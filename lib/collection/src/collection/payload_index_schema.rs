use std::collections::HashMap;
use std::path::{Path, PathBuf};
use serde::{Deserialize, Serialize};
use segment::types::{PayloadFieldSchema, PayloadKeyType};
use crate::collection::Collection;
use crate::save_on_disk::SaveOnDisk;

pub const PAYLOAD_INDEX_CONFIG_FILE: &str = "payload_index.json";


#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct PayloadIndexSchema {
    schema: HashMap<PayloadKeyType, PayloadFieldSchema>
}

impl Collection {
    fn payload_index_file(collection_path: &Path) -> PathBuf {
        collection_path.join(PAYLOAD_INDEX_CONFIG_FILE)
    }

    fn load_payload_index_schema(collection_path: &Path) -> SaveOnDisk<PayloadIndexSchema> {
        let payload_index_file = Self::payload_index_file(collection_path);

        let schema: SaveOnDisk<PayloadIndexSchema> = SaveOnDisk::load_or_init(payload_index_file).unwrap();



    }

}
