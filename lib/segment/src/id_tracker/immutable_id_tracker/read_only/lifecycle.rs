use std::io::Cursor;
use std::path::Path;

use common::bitvec::BitVec;
use common::generic_consts::Sequential;
use common::mmap::AdviceSetting;
use common::stored_bitslice::StoredBitSlice;
use common::universal_io::{
    OpenOptions, Populate, ReadRange, TypedStorage, UniversalRead, UniversalReadFileOps,
    UniversalReadFs,
};

use super::ReadOnlyImmutableIdTracker;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::compressed::versions_store::CompressedVersions;
use crate::id_tracker::immutable_id_tracker::deleted_storage::deleted_path;
use crate::id_tracker::immutable_id_tracker::mappings_storage::{load_mapping, mappings_path};
use crate::id_tracker::immutable_id_tracker::versions_storage::version_mapping_path;
use crate::types::SeqNumberType;

impl<S: UniversalRead> ReadOnlyImmutableIdTracker<S> {
    /// Open a read-only view over immutable ID tracker data at `segment_path`, threading every file
    /// open through `fs`. Read-only mirror of [`ImmutableIdTracker::open`]; it never writes.
    ///
    /// The `deleted` bitslice handle is kept for [`live_reload`](Self::live_reload); versions and
    /// mappings are immutable and read once into memory.
    ///
    /// [`ImmutableIdTracker::open`]: crate::id_tracker::immutable_id_tracker::ImmutableIdTracker::open
    ///
    /// Returns `Ok(None)` when the defining `id_tracker.mappings` file is absent
    /// (i.e. the segment is not in the immutable format). The probe uses
    /// `exists`, which a caching `fs` answers from its listing snapshot — a
    /// probe-by-open would consume the file's prefetched take-once handle
    /// that [`Self::open`] needs right after.
    pub fn try_open(
        fs: &impl UniversalReadFs<File = S>,
        segment_path: &Path,
    ) -> OperationResult<Option<Self>> {
        if !UniversalReadFileOps::exists(fs, &mappings_path(segment_path))? {
            return Ok(None);
        }
        Ok(Some(Self::open(fs, segment_path)?))
    }

    pub fn open(fs: &impl UniversalReadFs<File = S>, segment_path: &Path) -> OperationResult<Self> {
        let options = OpenOptions {
            writeable: false,
            need_sequential: false,
            populate: Populate::Blocking,
            advice: AdviceSetting::Global,
        };

        let deleted =
            StoredBitSlice::open(fs, deleted_path(segment_path), options, Default::default())?;
        let mut deleted_bitvec = BitVec::new();
        deleted_bitvec.extend_from_bitslice(deleted.read_all()?.as_ref());

        let internal_to_version_file = TypedStorage::<S, SeqNumberType>::new(fs.open(
            version_mapping_path(segment_path),
            options,
            Default::default(),
        )?);
        let internal_to_version =
            CompressedVersions::from_slice(&internal_to_version_file.read_whole()?);

        let mappings_file = fs.open(mappings_path(segment_path), options, Default::default())?;
        let mappings_bytes = mappings_file.read::<Sequential, u8>(ReadRange {
            byte_offset: 0,
            length: mappings_file.len::<u8>()?,
        })?;
        let mappings = load_mapping(Cursor::new(mappings_bytes.as_ref()), Some(deleted_bitvec))?;

        Ok(Self {
            path: segment_path.to_path_buf(),
            deleted,
            internal_to_version,
            mappings,
        })
    }
}
