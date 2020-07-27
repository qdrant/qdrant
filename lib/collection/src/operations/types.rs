use segment::types::{VectorElementType, PointIdType, TheMap, PayloadKeyType, PayloadType};
use serde;
use serde::{Deserialize, Serialize};
/// Type of vector in API
pub type VectorType = Vec<VectorElementType>;


#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct Record {
    pub id: PointIdType,
    pub payload: Option<TheMap<PayloadKeyType, PayloadType>>,
    pub vector: Option<Vec<VectorElementType>>
}
