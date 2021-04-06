use crate::types::{PayloadSchemaType, IntPayloadType, FloatPayloadType};
use crate::index::field_index::field_index::PayloadFieldIndexBuilder;
use crate::index::field_index::map_index::PersistedMapIndex;
use crate::index::field_index::numeric_index::PersistedNumericIndex;

pub fn index_selector(payload_type: &PayloadSchemaType) -> Vec<Box<dyn PayloadFieldIndexBuilder>> {
    match payload_type {
        PayloadSchemaType::Keyword => vec![Box::new(PersistedMapIndex::<String>::new())],
        PayloadSchemaType::Integer => vec![
            Box::new(PersistedMapIndex::<IntPayloadType>::new()),
            Box::new(PersistedNumericIndex::<IntPayloadType>::new())
        ],
        PayloadSchemaType::Float => vec![
            Box::new(PersistedNumericIndex::<FloatPayloadType>::new())
        ],
        PayloadSchemaType::Geo => vec![]
    }
}