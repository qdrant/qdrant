//! [`PayloadFieldIndexRead`] dispatch for [`ReadOnlyNumericIndexInner`].
//!
//! The query logic is shared with the writable index via the generic
//! [`query`](super::super::super::query) helpers over [`NumericIndexRead`];
//! this impl just plugs the read-only enum into them.

use blobstore::Blob;
use common::types::PointOffsetType;

use super::super::super::numeric_index_read::NumericIndexRead;
use super::super::super::{NumericIndexValue, query};
use super::ReadOnlyNumericIndexInner;
use crate::common::operation_error::OperationResult;
use crate::index::UniversalReadExt;
use crate::index::condition_checker::ConditionCheckerEnum;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndexRead,
};
use crate::types::{FieldCondition, PayloadKeyType};

impl<T: NumericIndexValue, S: UniversalReadExt> PayloadFieldIndexRead
    for ReadOnlyNumericIndexInner<T, S>
where
    Vec<T>: Blob,
{
    fn count_indexed_points(&self) -> OperationResult<usize> {
        Ok(self.get_points_count())
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        query::filter(self, condition)
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        query::estimate_cardinality(self, condition)
    }

    fn for_each_payload_block(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        query::for_each_payload_block(self, threshold, key, f)
    }

    fn condition_checker<'a>(
        &'a self,
        condition: &FieldCondition,
    ) -> OperationResult<Option<ConditionCheckerEnum<'a>>> {
        Ok(query::condition_checker(self, condition).map(T::condition_checker_read_only::<S>))
    }
}
