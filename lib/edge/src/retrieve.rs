use segment::types::{ExtendedPointId, WithPayloadInterface, WithVector};

use crate::Shard;

impl Shard {
    pub fn retrieve(
        &self,
        _ids: &[ExtendedPointId],
        _with_payload: Option<WithPayloadInterface>,
        _with_vector: Option<WithVector>,
    ) {
        todo!()
    }
}
