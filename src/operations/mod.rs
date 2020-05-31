
mod common;

pub mod tag_ops;
pub mod point_ops;
pub mod collection_ops;


pub enum Operations {
  PointOperation(point_ops::PointOps),
  TagOperation(tag_ops::TagOps),
  CollectionOperation(collection_ops::CollectionOps),
}
