use crate::index::field_index::map_index::PersistedMapIndex;
use crate::index::field_index::numeric_index::PersistedNumericIndex;
use crate::index::field_index::PayloadFieldIndexBuilder;
use crate::types::{FloatPayloadType, IntPayloadType, PayloadSchemaType};

pub fn index_selector(payload_type: &PayloadSchemaType) -> Vec<Box<dyn PayloadFieldIndexBuilder>> {
    match payload_type {
        PayloadSchemaType::Keyword => vec![Box::new(PersistedMapIndex::<String>::default())],
        PayloadSchemaType::Integer => vec![
            Box::new(PersistedMapIndex::<IntPayloadType>::default()),
            Box::new(PersistedNumericIndex::<IntPayloadType>::default()),
        ],
        PayloadSchemaType::Float => {
            vec![Box::new(
                PersistedNumericIndex::<FloatPayloadType>::default(),
            )]
        }
        PayloadSchemaType::Geo => vec![],
    }
}
