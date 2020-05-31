use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub enum Distance {
    Cosine,
    Euclid,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BaseIndexParams {
    pub distance: Distance,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PlainIndex {
    pub params: BaseIndexParams,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct HnswIndex {
    pub params: BaseIndexParams,
    pub m: usize,
    pub ef_construct: usize,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum IndexType {
    Plain(PlainIndex),
    Hnsw(HnswIndex),
}
