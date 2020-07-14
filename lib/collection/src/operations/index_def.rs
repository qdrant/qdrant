use serde::{Deserialize, Serialize};
use segment::types::Distance;



#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type",  content = "options")]
pub enum Indexes {
    Plain {},
    Hnsw {
        m: usize,
        ef_construct: usize
    },
}
