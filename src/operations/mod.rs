
use collection::operations::{point_ops, payload_ops};


pub enum Operations {
    PointOperation(point_ops::PointOps),
    PayloadOperation(payload_ops::PayloadOps),
    CollectionOperation(collection_ops::CollectionOps),
}
