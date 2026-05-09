mod payload_storage_read;

use common::universal_io::UniversalRead;
use gridstore::GridstoreReader;

use crate::types::Payload;

pub struct ReadOnlyPayloadStorage<S: UniversalRead> {
    storage: GridstoreReader<Payload, S>,
    populate: bool,
}
