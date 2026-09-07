use shard::scroll::ScrollRequestInternal;

use crate::requests::scroll::ScrollRequest;

impl From<ScrollRequest> for ScrollRequestInternal {
    fn from(request: ScrollRequest) -> Self {
        let ScrollRequest {
            offset,
            limit,
            filter,
            with_payload,
            with_vector,
            order_by,
        } = request;
        ScrollRequestInternal {
            offset,
            limit,
            filter,
            with_payload,
            with_vector,
            order_by,
        }
    }
}
