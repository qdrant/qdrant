use crate::index::field_index::btree_index::NumericIndex;
use crate::index::field_index::on_disk_geo_index::OnDiskGeoMapIndex;
use crate::index::field_index::on_disk_map_index::OnDiskMapIndex;
use crate::index::field_index::FieldIndex;
use crate::types::{FloatPayloadType, IntPayloadType, PayloadSchemaType};
use atomic_refcell::AtomicRefCell;
use rocksdb::DB;
use std::sync::Arc;

/// Selects index types based on field type
pub fn index_selector(
    field: &str,
    payload_type: &PayloadSchemaType,
    db: Arc<AtomicRefCell<DB>>,
) -> Vec<FieldIndex> {
    match payload_type {
        PayloadSchemaType::Keyword => {
            vec![FieldIndex::KeywordIndex(OnDiskMapIndex::new(db, field))]
        }
        PayloadSchemaType::Integer => vec![
            FieldIndex::IntMapIndex(OnDiskMapIndex::<IntPayloadType>::new(db.clone(), field)),
            FieldIndex::IntIndex(NumericIndex::<IntPayloadType>::new(db, field)),
        ],
        PayloadSchemaType::Float => {
            vec![FieldIndex::FloatIndex(
                NumericIndex::<FloatPayloadType>::new(db, field),
            )]
        }
        PayloadSchemaType::Geo => vec![FieldIndex::GeoIndex(OnDiskGeoMapIndex::new(db, field))],
    }
}
