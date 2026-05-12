use std::path::PathBuf;

use common::bitvec::BitVec;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use serde_json::Value;

use super::GeoMapIndex;
use super::mmap_geo_index::StoredGeoMapIndex;
use super::mutable_geo_index::InMemoryGeoMapIndex;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::{FieldIndexBuilderTrait, PayloadFieldIndex, ValueIndexer};

pub struct GeoMapIndexMmapBuilder {
    pub(super) path: PathBuf,
    pub(super) in_memory_index: InMemoryGeoMapIndex,
    pub(super) is_on_disk: bool,
    pub(super) deleted_points: BitVec,
}

impl FieldIndexBuilderTrait for GeoMapIndexMmapBuilder {
    type FieldIndexType = GeoMapIndex;

    fn init(&mut self) -> OperationResult<()> {
        Ok(())
    }

    fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let values = payload
            .iter()
            .flat_map(|value| <GeoMapIndex as ValueIndexer>::get_values(value))
            .collect::<Vec<_>>();
        self.in_memory_index
            .add_many_geo_points(id, &values, hw_counter)
    }

    fn finalize(self) -> OperationResult<Self::FieldIndexType> {
        Ok(GeoMapIndex::Storage(Box::new(StoredGeoMapIndex::build(
            self.in_memory_index,
            &self.path,
            self.is_on_disk,
            &self.deleted_points,
        )?)))
    }
}

pub struct GeoMapIndexGridstoreBuilder {
    dir: PathBuf,
    index: Option<GeoMapIndex>,
}

impl GeoMapIndexGridstoreBuilder {
    pub(super) fn new(dir: PathBuf) -> Self {
        Self { dir, index: None }
    }
}

impl FieldIndexBuilderTrait for GeoMapIndexGridstoreBuilder {
    type FieldIndexType = GeoMapIndex;

    fn init(&mut self) -> OperationResult<()> {
        assert!(
            self.index.is_none(),
            "index must be initialized exactly once",
        );
        self.index.replace(
            GeoMapIndex::new_gridstore(self.dir.clone(), true)?.ok_or_else(|| {
                OperationError::service_error("Failed to open GeoMapIndex after creating it")
            })?,
        );
        Ok(())
    }

    fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let Some(index) = &mut self.index else {
            return Err(OperationError::service_error(
                "GeoMapIndexGridstoreBuilder: index must be initialized before adding points",
            ));
        };
        index.add_point(id, payload, hw_counter)
    }

    fn finalize(mut self) -> OperationResult<Self::FieldIndexType> {
        let Some(index) = self.index.take() else {
            return Err(OperationError::service_error(
                "GeoMapIndexGridstoreBuilder: index must be initialized to finalize",
            ));
        };
        index.flusher()()?;
        Ok(index)
    }
}
