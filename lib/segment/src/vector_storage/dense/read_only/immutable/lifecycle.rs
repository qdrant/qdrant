use std::path::Path;

use common::bitvec::BitVec;
use common::mmap::AdviceSetting;
use common::stored_bitslice::StoredBitSlice;
use common::universal_io::{OpenOptions, Populate, UniversalRead, UniversalReadFs};

use super::ReadOnlyImmutableDenseVectorStorage;
use crate::common::flags::in_memory_bitvec_flags::InMemoryBitvecFlags;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::types::Distance;
use crate::vector_storage::dense::dense_vector_storage::{DELETED_PATH, VECTORS_PATH};
use crate::vector_storage::dense::immutable_dense_vectors::{
    ImmutableDenseVectorData, deleted_mmap_data_start,
};

/// Read-only mmap options: never writable, lazily paged, nothing populated.
const READ_ONLY_OPTIONS: OpenOptions = OpenOptions {
    writeable: false,
    need_sequential: false,
    populate: Populate::No,
    advice: AdviceSetting::Global,
};

impl<T: PrimitiveVectorElement, S: UniversalRead> ReadOnlyImmutableDenseVectorStorage<T, S> {
    /// Open the read-only counterpart of the immutable dense storage at `path`,
    /// threading every file open through `fs`; reads the existing layout but
    /// creates and writes nothing. `populate` warms the vector data.
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        path: &Path,
        dim: usize,
        distance: Distance,
        populate: Populate,
    ) -> OperationResult<Self> {
        let vectors = ImmutableDenseVectorData::open(fs, &path.join(VECTORS_PATH), dim, populate)?;
        let deleted = open_deleted_flags::<S>(fs, &path.join(DELETED_PATH), vectors.num_vectors)?;

        Ok(Self {
            vectors,
            deleted,
            distance,
            populate,
        })
    }
}

/// Read the immutable storage's `deleted.dat` through `fs` into an in-memory flag
/// set. The file holds a header padded to `deleted_mmap_data_start()` bytes
/// followed by the deletion `BitSlice`; the leading header bits are dropped and
/// `num_vectors` deletion flags are kept.
fn open_deleted_flags<S: UniversalRead>(
    fs: &impl UniversalReadFs<File = S>,
    deleted_path: &Path,
    num_vectors: usize,
) -> OperationResult<InMemoryBitvecFlags> {
    let stored =
        StoredBitSlice::<S>::open(fs, deleted_path, READ_ONLY_OPTIONS, Default::default())?;
    let all = stored.read_all()?;

    let start = deleted_mmap_data_start() * 8;
    let bits = all.get(start..start + num_vectors).ok_or_else(|| {
        OperationError::service_error(format!(
            "Deleted flags file {} holds fewer than {num_vectors} bits",
            deleted_path.display(),
        ))
    })?;

    Ok(InMemoryBitvecFlags::from_bitvec(BitVec::from_bitslice(
        bits,
    )))
}
