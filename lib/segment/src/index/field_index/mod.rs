use crate::index::field_index::numeric_index::PersistedNumericIndex;
use crate::types::{IntPayloadType, FloatPayloadType};
use serde::{Deserialize, Serialize};

pub mod numeric_index;
pub mod index_builder;


#[derive(Debug)]
pub struct Estimation {
    pub min: usize,
    pub exp: usize,
    pub max: usize
}


#[derive(Serialize, Deserialize)]
pub enum FieldIndex {
    IntIndex(PersistedNumericIndex<IntPayloadType>),
    FloatIndex(PersistedNumericIndex<FloatPayloadType>),
}