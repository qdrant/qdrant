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
pub enum Indexes {
    PlainIndex {
        params: BaseIndexParams,
    },
    HnswIndex {
        params: BaseIndexParams,
        m: usize,
        ef_construct: usize,
    },
}
