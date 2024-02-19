use std::sync::Arc;

use parking_lot::RwLock;
use rocksdb::DB;

use super::binary_index::BinaryIndex;
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
    is_appendable: bool,
) -> Vec<FieldIndex> {
    match payload_schema {
        PayloadFieldSchema::FieldType(payload_type) => match payload_type {
            PayloadSchemaType::Keyword => {
                vec![FieldIndex::KeywordIndex(MapIndex::new(
                    db,
                    field,
                    is_appendable,
                ))]
            }
            PayloadSchemaType::Integer => vec![
                FieldIndex::IntMapIndex(MapIndex::new(db.clone(), field, is_appendable)),
                FieldIndex::IntIndex(NumericIndex::<IntPayloadType>::new(
                    db,
                    field,
                    is_appendable,
                )),
            ],
            PayloadSchemaType::Float => {
                vec![FieldIndex::FloatIndex(
                    NumericIndex::<FloatPayloadType>::new(db, field, is_appendable),
                )]
            }
            PayloadSchemaType::Geo => vec![FieldIndex::GeoIndex(GeoMapIndex::new(
                db,
                field,
                is_appendable,
            ))],
            PayloadSchemaType::Text => vec![FieldIndex::FullTextIndex(FullTextIndex::new(
                db,
                Default::default(),
                field,
                is_appendable,
            ))],
            PayloadSchemaType::Bool => vec![FieldIndex::BinaryIndex(BinaryIndex::new(db, field))],
            PayloadSchemaType::Datetime => {
                vec![FieldIndex::DatetimeIndex(
                    NumericIndex::<IntPayloadType>::new(db, field, is_appendable),
                )]
            }
        },
        PayloadFieldSchema::FieldParams(payload_params) => match payload_params {
            PayloadSchemaParams::Text(text_index_params) => vec![FieldIndex::FullTextIndex(
                FullTextIndex::new(db, text_index_params.clone(), field, is_appendable),
            )],
            PayloadSchemaParams::Integer(integer_params) => {
                let lookup = integer_params.lookup.then(|| {
                    FieldIndex::IntMapIndex(MapIndex::new(db.clone(), field, is_appendable))
                });
                let range = integer_params.range.then(|| {
                    FieldIndex::IntIndex(NumericIndex::<IntPayloadType>::new(
                        db,
                        field,
                        is_appendable,
                    ))
                });
                lookup.into_iter().chain(range).collect()
            }
        },
    }
}
