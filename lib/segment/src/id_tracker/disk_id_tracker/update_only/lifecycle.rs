//! Opening the append-only-backend delete writer over an existing
//! [`DiskIdTracker`](super::super::DiskIdTracker) segment.

use std::path::Path;

use common::bitvec::BitVec;
use common::mmap::AdviceSetting;
use common::stored_bitslice::StoredBitSlice;
use common::universal_io::{OpenOptions, Populate, UniversalAppend};

use super::UpdateOnlyDiskIdTracker;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::immutable_id_tracker::deleted_path;

impl<S: UniversalAppend> UpdateOnlyDiskIdTracker<S> {
    /// Open the `deleted` file of a segment already built by
    /// [`DiskIdTracker`](super::super::DiskIdTracker); the mapping and
    /// `versions` files are untouched by this type and stay on disk.
    pub fn open(fs: &S::Fs, segment_path: &Path) -> OperationResult<Self> {
        let open_options = OpenOptions {
            writeable: false,
            need_sequential: false,
            populate: Populate::Blocking,
            advice: AdviceSetting::Global,
        };

        let deleted_storage = StoredBitSlice::open(
            fs,
            deleted_path(segment_path),
            open_options,
            Default::default(),
        )?;
        let deleted: BitVec = deleted_storage.read_all()?.into_owned();

        Ok(Self {
            path: segment_path.to_path_buf(),
            fs: fs.clone(),
            deleted,
        })
    }
}
