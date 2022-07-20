use std::sync::Arc;

use atomic_refcell::AtomicRefCell;

use crate::common::rocksdb_operations::Database;
use crate::index::field_index::geo_index::GeoMapIndex;
use crate::index::field_index::map_index::MapIndex;
use crate::index::field_index::numeric_index::NumericIndex;
use crate::index::field_index::FieldIndex;
use crate::types::{FloatPayloadType, IntPayloadType, PayloadSchemaType};

/// Selects index types based on field type
pub fn index_selector(
    field: &str,
    payload_type: &PayloadSchemaType,
    database: Arc<AtomicRefCell<Database>>,
) -> Vec<FieldIndex> {
    match payload_type {
        PayloadSchemaType::Keyword => {
            vec![FieldIndex::KeywordIndex(MapIndex::new(database, field))]
        }
        PayloadSchemaType::Integer => vec![
            FieldIndex::IntMapIndex(MapIndex::<IntPayloadType>::new(database.clone(), field)),
            FieldIndex::IntIndex(NumericIndex::<IntPayloadType>::new(database, field)),
        ],
        PayloadSchemaType::Float => {
            vec![FieldIndex::FloatIndex(
                NumericIndex::<FloatPayloadType>::new(database, field),
            )]
        }
        PayloadSchemaType::Geo => vec![FieldIndex::GeoIndex(GeoMapIndex::new(database, field))],
    }
}
