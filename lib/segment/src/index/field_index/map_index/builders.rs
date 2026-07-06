use std::borrow::Borrow;
use std::collections::hash_map::Entry;
use std::mem::size_of_val;
use std::path::PathBuf;

use ahash::HashMap;
use common::bitvec::BitVec;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::{MmapFs, Populate};
use gridstore::Blob;
use itertools::Itertools;
use serde_json::Value;

use super::MapIndex;
use super::key::MapIndexKey;
use super::on_disk_map_index::OnDiskMapIndex;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::map_index::immutable_map_index::ImmutableMapIndex;
use crate::index::field_index::{FieldIndexBuilderTrait, PayloadFieldIndex, ValueIndexer};

pub struct MapIndexBuilder<N: MapIndexKey + ?Sized>(pub(super) MapIndex<N>)
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync;

impl<N: MapIndexKey + ?Sized> FieldIndexBuilderTrait for MapIndexBuilder<N>
where
    MapIndex<N>: PayloadFieldIndex + ValueIndexer,
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    type FieldIndexType = MapIndex<N>;

    fn init(&mut self) -> OperationResult<()> {
        match &mut self.0 {
            MapIndex::Mutable(index) => index.clear(),
            MapIndex::Immutable(_) => unreachable!(),
            MapIndex::OnDisk(_) => unreachable!(),
        }
    }

    fn add_point(
        &mut self,
        id: PointOffsetType,
        values: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.0.add_point(id, values, hw_counter)
    }

    fn finalize(self) -> OperationResult<Self::FieldIndexType> {
        Ok(self.0)
    }
}

pub struct MapIndexMmapBuilder<N: MapIndexKey + ?Sized> {
    pub(super) path: PathBuf,
    pub(super) point_to_values: Vec<Vec<<N as MapIndexKey>::Owned>>,
    pub(super) values_to_points: HashMap<<N as MapIndexKey>::Owned, Vec<PointOffsetType>>,
    pub(super) is_on_disk: bool,
    pub(super) deleted_points: BitVec,
    pub(super) prefix_index: bool,
}

impl<N: MapIndexKey + ?Sized> FieldIndexBuilderTrait for MapIndexMmapBuilder<N>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
    MapIndex<N>: PayloadFieldIndex + ValueIndexer,
    <MapIndex<N> as ValueIndexer>::ValueType: Into<<N as MapIndexKey>::Owned>,
{
    type FieldIndexType = MapIndex<N>;

    fn init(&mut self) -> OperationResult<()> {
        Ok(())
    }

    fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let mut flatten_values: Vec<_> = vec![];
        for value in payload {
            let payload_values = <MapIndex<N> as ValueIndexer>::get_values(value);
            flatten_values.extend(payload_values);
        }
        let flatten_values: Vec<<N as MapIndexKey>::Owned> =
            flatten_values.into_iter().map_into().unique().collect();

        if self.point_to_values.len() <= id as usize {
            self.point_to_values.resize_with(id as usize + 1, Vec::new);
        }

        self.point_to_values[id as usize].extend(flatten_values.clone());

        let mut hw_cell_wb = hw_counter
            .payload_index_io_write_counter()
            .write_back_counter();

        for value in flatten_values {
            let entry = self.values_to_points.entry(value);

            if let Entry::Vacant(e) = &entry {
                let size = N::stored_size(e.key().borrow());
                hw_cell_wb.incr_delta(size);
            }

            hw_cell_wb.incr_delta(size_of_val(&id));
            entry.or_default().push(id);
        }

        Ok(())
    }

    fn finalize(self) -> OperationResult<Self::FieldIndexType> {
        let populate = Populate::from(!self.is_on_disk);
        let on_disk_index = OnDiskMapIndex::build(
            &MmapFs,
            &self.path,
            self.point_to_values,
            self.values_to_points,
            populate,
            &self.deleted_points,
            self.prefix_index,
        )?;

        let index = if self.is_on_disk {
            MapIndex::OnDisk(on_disk_index)
        } else {
            MapIndex::Immutable(ImmutableMapIndex::load_from_on_disk(on_disk_index)?)
        };

        Ok(index)
    }
}

pub struct MapIndexGridstoreBuilder<N: MapIndexKey + ?Sized>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    dir: PathBuf,
    index: Option<MapIndex<N>>,
    prefix_index: bool,
}

impl<N: MapIndexKey + ?Sized> MapIndexGridstoreBuilder<N>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    pub(super) fn new(dir: PathBuf, prefix_index: bool) -> Self {
        Self {
            dir,
            index: None,
            prefix_index,
        }
    }
}

impl<N: MapIndexKey + ?Sized> FieldIndexBuilderTrait for MapIndexGridstoreBuilder<N>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
    MapIndex<N>: PayloadFieldIndex + ValueIndexer,
    <MapIndex<N> as ValueIndexer>::ValueType: Into<<N as MapIndexKey>::Owned>,
{
    type FieldIndexType = MapIndex<N>;

    fn init(&mut self) -> OperationResult<()> {
        assert!(
            self.index.is_none(),
            "index must be initialized exactly once",
        );
        self.index.replace(
            MapIndex::new_mutable(self.dir.clone(), true, self.prefix_index)?.ok_or_else(|| {
                OperationError::service_error("Failed to create mutable map index")
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
                "MapIndexGridstoreBuilder: index must be initialized before adding points",
            ));
        };
        index.add_point(id, payload, hw_counter)
    }

    fn finalize(mut self) -> OperationResult<Self::FieldIndexType> {
        let Some(index) = self.index.take() else {
            return Err(OperationError::service_error(
                "MapIndexGridstoreBuilder: index must be initialized to finalize",
            ));
        };
        index.flusher()()?;
        Ok(index)
    }
}
