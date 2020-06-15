mod common;

pub mod collection_ops;
pub mod payload_ops;
pub mod point_ops;

pub enum Operations {
    PointOperation(point_ops::PointOps),
    PayloadOperation(payload_ops::PayloadOps),
    CollectionOperation(collection_ops::CollectionOps),
}
