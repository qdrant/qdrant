use std::path::PathBuf;

use common::bitvec::BitVec;
use common::types::PointOffsetType;
use common::universal_io::{MmapFs, Populate};
use serde_json::Value;

use super::GeoIndex;
use super::mutable_geo_index::InMemoryGeoIndex;
use super::on_disk_geo_index::OnDiskGeoIndex;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::geo_index::immutable_geo_index::ImmutableGeoIndex;
use crate::index::field_index::{FieldIndexBuilderTrait, PayloadFieldIndex, ValueIndexer};

pub struct GeoIndexMmapBuilder {
    pub(super) path: PathBuf,
    pub(super) in_memory_index: InMemoryGeoIndex,
    pub(super) is_on_disk: bool,
    pub(super) deleted_points: BitVec,
}

impl FieldIndexBuilderTrait for GeoIndexMmapBuilder {
    type FieldIndexType = GeoIndex;

    fn init(&mut self) -> OperationResult<()> {
        Ok(())
    }

    fn add_point(&mut self, id: PointOffsetType, payload: &[&Value]) -> OperationResult<()> {
        let values = payload
            .iter()
            .flat_map(|value| <GeoIndex as ValueIndexer>::get_values(value))
            .collect::<Vec<_>>();
        self.in_memory_index.add_many_geo_points(id, values)
    }

    fn finalize(self) -> OperationResult<Self::FieldIndexType> {
        let populate = Populate::from(!self.is_on_disk);
        let on_disk_index = OnDiskGeoIndex::build(
            &MmapFs,
            self.in_memory_index,
            &self.path,
            populate,
            &self.deleted_points,
        )?;

        let index = if self.is_on_disk {
            GeoIndex::OnDisk(on_disk_index)
        } else {
            GeoIndex::Immutable(ImmutableGeoIndex::load_from_on_disk(on_disk_index)?)
        };
        Ok(index)
    }
}

pub struct GeoIndexGridstoreBuilder {
    dir: PathBuf,
    index: Option<GeoIndex>,
}

impl GeoIndexGridstoreBuilder {
    pub(super) fn new(dir: PathBuf) -> Self {
        Self { dir, index: None }
    }

    /// Don't journal the value mappings of the built index, see
    /// [`Blobstore::disable_journal`](blobstore::Blobstore::disable_journal). Call after `init`.
    pub(crate) fn disable_journal(&mut self) {
        if let Some(GeoIndex::Mutable(index)) = &mut self.index {
            index.storage.disable_journal();
        }
    }
}

impl FieldIndexBuilderTrait for GeoIndexGridstoreBuilder {
    type FieldIndexType = GeoIndex;

    fn init(&mut self) -> OperationResult<()> {
        assert!(
            self.index.is_none(),
            "index must be initialized exactly once",
        );
        self.index.replace(
            GeoIndex::new_mutable(self.dir.clone(), true)?.ok_or_else(|| {
                OperationError::service_error("Failed to open GeoIndex after creating it")
            })?,
        );
        Ok(())
    }

    fn add_point(&mut self, id: PointOffsetType, payload: &[&Value]) -> OperationResult<()> {
        let Some(index) = &mut self.index else {
            return Err(OperationError::service_error(
                "GeoIndexGridstoreBuilder: index must be initialized before adding points",
            ));
        };
        index.add_point(id, payload)
    }

    fn finalize(mut self) -> OperationResult<Self::FieldIndexType> {
        let Some(index) = self.index.take() else {
            return Err(OperationError::service_error(
                "GeoIndexGridstoreBuilder: index must be initialized to finalize",
            ));
        };
        index.flusher()()?;
        Ok(index)
    }
}
