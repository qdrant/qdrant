use crate::index::field_index::geo_index::PersistedGeoMapIndex;
use crate::index::field_index::numeric_index::PersistedNumericIndex;
use crate::index::field_index::on_disk_map_index::OnDiskMapIndex;
use crate::index::field_index::PayloadFieldIndexBuilder;
use crate::types::{FloatPayloadType, IntPayloadType, PayloadSchemaType};
use atomic_refcell::AtomicRefCell;
use rocksdb::DB;
use std::sync::Arc;

/// Selects index types based on field type
pub fn index_selector(
    payload_type: &PayloadSchemaType,
    store_cf_name: &str,
    store: Arc<AtomicRefCell<DB>>,
) -> Vec<Box<dyn PayloadFieldIndexBuilder>> {
    match payload_type {
        PayloadSchemaType::Keyword => vec![Box::new(OnDiskMapIndex::<String>::new(
            store,
            store_cf_name,
        ))],
        PayloadSchemaType::Integer => vec![
            Box::new(OnDiskMapIndex::<IntPayloadType>::new(store, store_cf_name)),
            Box::new(PersistedNumericIndex::<IntPayloadType>::default()),
        ],
        PayloadSchemaType::Float => {
            vec![Box::new(
                PersistedNumericIndex::<FloatPayloadType>::default(),
            )]
        }
        PayloadSchemaType::Geo => vec![Box::new(PersistedGeoMapIndex::default())],
    }
}
