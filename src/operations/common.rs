use serde::{Serialize, Deserialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct CollectionUpdateInfo {
  collection: String, // To which collection this operation is applied.
  order_id: usize, // Sequence number of operation. All user operations should be executed in this order.
}
