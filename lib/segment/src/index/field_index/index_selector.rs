use std::sync::Arc;

use parking_lot::RwLock;
use rocksdb::DB;

use super::binary_index::BinaryIndex;
use crate::index::field_index::full_text_index::text_index::FullTextIndex;
use crate::index::field_index::geo_index::GeoMapIndex;
use crate::index::field_index::map_index::MapIndex;
use crate::index::field_index::numeric_index::NumericIndex;
use crate::index::field_index::FieldIndex;
use crate::json_path::JsonPath;
use crate::types::{FloatPayloadType, IntPayloadType, PayloadFieldSchema, PayloadSchemaParams};

/// Selects index types based on field type
pub fn index_selector(
    field: &JsonPath,
    payload_schema: &PayloadFieldSchema,
    db: Arc<RwLock<DB>>,
    is_appendable: bool,
) -> Vec<FieldIndex> {
    let field: String = field.to_string();
    let field = field.as_str();

    match payload_schema.expand().as_ref() {
        PayloadSchemaParams::Keyword(_) => vec![FieldIndex::KeywordIndex(MapIndex::new(
            db,
            field,
            is_appendable,
        ))],
        PayloadSchemaParams::Integer(integer_params) => {
            let lookup = integer_params
                .lookup
                .then(|| FieldIndex::IntMapIndex(MapIndex::new(db.clone(), field, is_appendable)));
            let range = integer_params.range.then(|| {
                FieldIndex::IntIndex(NumericIndex::<IntPayloadType>::new(
                    db,
                    field,
                    is_appendable,
                ))
            });
            lookup.into_iter().chain(range).collect()
        }
        PayloadSchemaParams::Float(_) => {
            vec![FieldIndex::FloatIndex(
                NumericIndex::<FloatPayloadType>::new(db, field, is_appendable),
            )]
        }
        PayloadSchemaParams::Geo(_) => vec![FieldIndex::GeoIndex(GeoMapIndex::new(
            db,
            field,
            is_appendable,
        ))],
        PayloadSchemaParams::Text(text_index_params) => vec![FieldIndex::FullTextIndex(
            FullTextIndex::new(db, text_index_params.clone(), field, is_appendable),
        )],
        PayloadSchemaParams::Bool(_) => {
            vec![FieldIndex::BinaryIndex(BinaryIndex::new(db, field))]
        }
        PayloadSchemaParams::Datetime(_) => {
            vec![FieldIndex::DatetimeIndex(
                NumericIndex::<IntPayloadType>::new(db, field, is_appendable),
            )]
        }
    }
}
