pub mod chunked_vector_storage;

use std::borrow::Cow;

use common::generic_consts::AccessPattern;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use super::appendable_mmap_multi_dense_vector_storage::MultivectorMmapOffset;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::vector_storage::chunked_vectors::ChunkedVectorsRead;

pub fn iter_vectors<'a, P, T, U, S>(
    offsets: &'a ChunkedVectorsRead<MultivectorMmapOffset, S>,
    vectors: &'a ChunkedVectorsRead<T, S>,
    keys: impl IntoIterator<Item = (U, PointOffsetType)>,
) -> impl Iterator<Item = (U, Cow<'a, [T]>)>
where
    P: AccessPattern,
    T: PrimitiveVectorElement,
    S: UniversalRead,
{
    let point_offsets = keys
        .into_iter()
        .map(|(user_data, point_offset)| (user_data, point_offset as _, 1));

    let vector_offsets =
        offsets
            .iter_vectors::<P, _>(point_offsets)
            .map(|(user_data, multi_offset)| {
                let &[multi_offset] = multi_offset.as_ref() else {
                    unreachable!("multi-vector offsets are stored as vectors of length 1");
                };

                let MultivectorMmapOffset {
                    offset,
                    count,
                    capacity: _,
                } = multi_offset;

                (user_data, offset, count)
            });

    vectors.iter_vectors::<P, _>(vector_offsets)
}
