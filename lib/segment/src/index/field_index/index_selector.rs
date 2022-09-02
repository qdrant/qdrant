use std::sync::Arc;

use parking_lot::RwLock;
use rocksdb::DB;

use crate::index::field_index::full_text_index::text_index::FullTextIndex;
use crate::index::field_index::geo_index::GeoMapIndex;
use crate::index::field_index::map_index::MapIndex;
use crate::index::field_index::numeric_index::NumericIndex;
use crate::index::field_index::FieldIndex;
use crate::types::{
    FloatPayloadType, IntPayloadType, PayloadFieldSchema, PayloadSchemaParams, PayloadSchemaType,
};

/// Selects index types based on field type
pub fn index_selector(
    field: &str,
    payload_schema: &PayloadFieldSchema,
    db: Arc<RwLock<DB>>,
) -> Vec<FieldIndex> {
    match payload_schema {
        PayloadFieldSchema::FieldType(payload_type) => match payload_type {
            PayloadSchemaType::Keyword => {
                vec![FieldIndex::KeywordIndex(MapIndex::new(db, field))]
            }
            PayloadSchemaType::Integer => vec![
                FieldIndex::IntMapIndex(MapIndex::<IntPayloadType>::new(db.clone(), field)),
                FieldIndex::IntIndex(NumericIndex::<IntPayloadType>::new(db, field)),
            ],
            PayloadSchemaType::Float => {
                vec![FieldIndex::FloatIndex(
                    NumericIndex::<FloatPayloadType>::new(db, field),
                )]
            }
            PayloadSchemaType::Geo => vec![FieldIndex::GeoIndex(GeoMapIndex::new(db, field))],
            PayloadSchemaType::Text => vec![FieldIndex::FullTextIndex(FullTextIndex::new(
                db,
                Default::default(),
                field,
            ))],
        },
        PayloadFieldSchema::FieldParams(payload_params) => match payload_params {
            PayloadSchemaParams::Text(text_index_params) => vec![FieldIndex::FullTextIndex(
                FullTextIndex::new(db, text_index_params.clone(), field),
            )],
        },
    }
}
