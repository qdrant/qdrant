pub mod types;
pub mod point_ops;
pub mod payload_ops;
pub mod index_def;


pub enum CollectionUpdateOperations {
    PointOperation(point_ops::PointOps),
    PayloadOperation(payload_ops::PayloadOps),
}
