use std::marker::PhantomData;
use std::path::PathBuf;

use common::bitvec::BitVec;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use gridstore::Blob;
use serde_json::Value;

use super::Encodable;
use super::index::{NumericIndex, NumericIndexIntoInnerValue};
use super::mmap_numeric_index::MmapNumericIndex;
use super::mutable_numeric_index::InMemoryNumericIndex;
use super::storage::NumericIndexInner;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::numeric_point::Numericable;
use crate::index::field_index::stored_point_to_values::StoredValue;
use crate::index::field_index::{FieldIndexBuilderTrait, ValueIndexer};

pub struct NumericIndexBuilder<T: Encodable + Numericable + StoredValue + Send + Sync + Default, P>(
    NumericIndex<T, P>,
)
where
    NumericIndex<T, P>: ValueIndexer<ValueType = P>,
    Vec<T>: Blob;

impl<T: Encodable + Numericable + StoredValue + Send + Sync + Default, P> FieldIndexBuilderTrait
    for NumericIndexBuilder<T, P>
where
    NumericIndex<T, P>: ValueIndexer<ValueType = P>,
    Vec<T>: Blob,
{
    type FieldIndexType = NumericIndex<T, P>;

    fn init(&mut self) -> OperationResult<()> {
        match &mut self.0.inner {
            NumericIndexInner::Mutable(index) => index.clear(),
            NumericIndexInner::Immutable(_) => unreachable!(),
            NumericIndexInner::Mmap(_) => unreachable!(),
        }
    }

    fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.0.add_point(id, payload, hw_counter)
    }

    fn finalize(self) -> OperationResult<Self::FieldIndexType> {
        self.0.inner.flusher()()?;
        Ok(self.0)
    }
}

pub struct NumericIndexMmapBuilder<T, P>
where
    T: Encodable + Numericable + StoredValue + Send + Sync + Default,
    NumericIndex<T, P>: ValueIndexer<ValueType = P> + NumericIndexIntoInnerValue<T, P>,
    Vec<T>: Blob,
{
    path: PathBuf,
    in_memory_index: InMemoryNumericIndex<T>,
    is_on_disk: bool,
    deleted_points: BitVec,
    _phantom: PhantomData<P>,
}

impl<T, P> NumericIndexMmapBuilder<T, P>
where
    T: Encodable + Numericable + StoredValue + Send + Sync + Default,
    NumericIndex<T, P>: ValueIndexer<ValueType = P> + NumericIndexIntoInnerValue<T, P>,
    Vec<T>: Blob,
{
    pub(super) fn new(path: PathBuf, is_on_disk: bool, deleted_points: BitVec) -> Self {
        Self {
            path,
            in_memory_index: InMemoryNumericIndex::default(),
            is_on_disk,
            deleted_points,
            _phantom: PhantomData,
        }
    }
}

impl<T: Encodable + Numericable + StoredValue + Send + Sync + Default, P> FieldIndexBuilderTrait
    for NumericIndexMmapBuilder<T, P>
where
    NumericIndex<T, P>: ValueIndexer<ValueType = P> + NumericIndexIntoInnerValue<T, P>,
    Vec<T>: Blob,
{
    type FieldIndexType = NumericIndex<T, P>;

    fn init(&mut self) -> OperationResult<()> {
        Ok(())
    }

    fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.in_memory_index.remove_point(id);
        let mut flatten_values: Vec<_> = vec![];
        for value in payload {
            let payload_values = <NumericIndex<T, P> as ValueIndexer>::get_values(value);
            flatten_values.extend(payload_values);
        }
        let flatten_values = flatten_values
            .into_iter()
            .map(NumericIndex::into_inner_value)
            .collect();

        hw_counter
            .payload_index_io_write_counter()
            .incr_delta(size_of_val(&flatten_values));

        self.in_memory_index.add_many_to_list(id, flatten_values);
        Ok(())
    }

    fn finalize(self) -> OperationResult<Self::FieldIndexType> {
        let inner = MmapNumericIndex::build(
            self.in_memory_index,
            &self.path,
            self.is_on_disk,
            &self.deleted_points,
        )?;
        Ok(NumericIndex {
            inner: NumericIndexInner::Mmap(inner),
            _phantom: PhantomData,
        })
    }
}

pub struct NumericIndexGridstoreBuilder<
    T: Encodable + Numericable + StoredValue + Send + Sync + Default,
    P,
> where
    NumericIndex<T, P>: ValueIndexer<ValueType = P>,
    Vec<T>: Blob,
{
    dir: PathBuf,
    index: Option<NumericIndex<T, P>>,
}

impl<T: Encodable + Numericable + StoredValue + Send + Sync + Default, P>
    NumericIndexGridstoreBuilder<T, P>
where
    NumericIndex<T, P>: ValueIndexer<ValueType = P>,
    Vec<T>: Blob,
{
    pub(super) fn new(dir: PathBuf) -> Self {
        Self { dir, index: None }
    }
}

impl<T: Encodable + Numericable + StoredValue + Send + Sync + Default, P> FieldIndexBuilderTrait
    for NumericIndexGridstoreBuilder<T, P>
where
    NumericIndex<T, P>: ValueIndexer<ValueType = P>,
    Vec<T>: Blob,
{
    type FieldIndexType = NumericIndex<T, P>;

    fn init(&mut self) -> OperationResult<()> {
        assert!(
            self.index.is_none(),
            "index must be initialized exactly once",
        );
        self.index.replace(
            NumericIndex::new_gridstore(self.dir.clone(), true)?
                // unwrap safety: cannot fail because create_if_missing is true
                .unwrap(),
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
                "NumericIndexGridstoreBuilder: index must be initialized before adding points",
            ));
        };
        index.add_point(id, payload, hw_counter)
    }

    fn finalize(mut self) -> OperationResult<Self::FieldIndexType> {
        let Some(index) = self.index.take() else {
            return Err(OperationError::service_error(
                "NumericIndexGridstoreBuilder: index must be initialized to finalize",
            ));
        };
        index.inner.flusher()()?;
        Ok(index)
    }
}
