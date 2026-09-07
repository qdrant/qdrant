use std::borrow::Cow;

use common::generic_consts::AccessPattern;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use crate::common::operation_error::OperationResult;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::vector_storage::dense::immutable_dense_vectors::ImmutableDenseVectorData;

pub trait DenseVectorBlob {
    type Element: PrimitiveVectorElement;
    type File: UniversalRead;

    fn dim(&self) -> usize;

    fn num_vectors(&self) -> usize;

    fn get_vector_opt<P: AccessPattern>(
        &self,
        key: PointOffsetType,
    ) -> Option<Cow<'_, [Self::Element]>>;

    fn for_each_in_batch<F: FnMut(usize, &[Self::Element])>(
        &self,
        keys: &[PointOffsetType],
        f: F,
    ) -> OperationResult<()>;
}

impl<T: PrimitiveVectorElement, S: UniversalRead> DenseVectorBlob
    for ImmutableDenseVectorData<T, S>
{
    type Element = T;
    type File = S;

    fn dim(&self) -> usize {
        self.dim
    }

    fn num_vectors(&self) -> usize {
        self.num_vectors
    }

    fn get_vector_opt<P: AccessPattern>(&self, key: PointOffsetType) -> Option<Cow<'_, [T]>> {
        ImmutableDenseVectorData::get_vector_opt::<P>(self, key)
    }

    fn for_each_in_batch<F: FnMut(usize, &[T])>(
        &self,
        keys: &[PointOffsetType],
        f: F,
    ) -> OperationResult<()> {
        ImmutableDenseVectorData::for_each_in_batch(self, keys, f)
    }
}
