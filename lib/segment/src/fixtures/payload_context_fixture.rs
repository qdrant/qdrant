use std::path::Path;
use crate::payload_storage::simple_payload_storage::SimplePayloadStorage;

pub fn create_payload_storage(path: &Path) -> SimplePayloadStorage {
    let payload_storage = SimplePayloadStorage::open(path).unwrap();


    payload_storage
}