use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub enum Distance {
  Cosine,
  Euclid
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BaseIndexParams {
  distance: Distance
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PlainIndex {
  params: BaseIndexParams,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct HnswIndex {
  params: BaseIndexParams,
  m: usize,
  ef_construct: usize
}

#[derive(Debug, Deserialize, Serialize)]
pub enum IndexType {
  PlainIndex, HnswIndex
}

