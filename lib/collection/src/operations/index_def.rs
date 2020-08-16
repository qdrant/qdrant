use serde::{Deserialize, Serialize};




#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type",  content = "options")]
pub enum Indexes {
    Plain {},
    Hnsw {
        m: usize,
        ef_construct: usize
    },
}
