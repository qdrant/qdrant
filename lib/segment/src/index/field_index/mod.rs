use crate::index::field_index::numeric_index::PersistedNumericIndex;
use crate::types::{IntPayloadType, FloatPayloadType};
use serde::{Deserialize, Serialize};
use crate::index::field_index::map_index::PersistedMapIndex;

pub mod numeric_index;
pub mod index_builder;
pub mod geo_index;
pub mod map_index;


#[derive(Debug)]
pub struct Estimation {
    pub min: usize,
    pub exp: usize,
    pub max: usize
}


#[derive(Serialize, Deserialize)]
pub enum FieldIndex {
    IntIndex(PersistedNumericIndex<IntPayloadType>),
    IntMapIndex(PersistedMapIndex<IntPayloadType>),
    KeywordIndex(PersistedMapIndex<String>),
    FloatIndex(PersistedNumericIndex<FloatPayloadType>),

}