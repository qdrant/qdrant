mod payload_storage_read;

use gridstore::GridstoreReader;

use crate::types::Payload;

pub struct ReadOnlyPayloadStorage {
    storage: GridstoreReader<Payload>,
    populate: bool,
}
