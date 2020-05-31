use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct CollectionUpdateInfo {
    pub collection: String, // To which collection this operation is applied.
    pub order_id: usize, // Sequence number of operation. All user operations should be executed in this order.
}
