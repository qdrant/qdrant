use std::path::PathBuf;

use common::mmap::Advice::Normal;
use common::mmap::AdviceSetting;
use common::types::PointOffsetType;
use common::universal_io::{OpenOptions, Populate, UniversalRead, UniversalReadFs};

use super::ReadOnlyAppendableIdTracker;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::mutable_id_tracker::mappings_storage::mappings_path;
use crate::id_tracker::mutable_id_tracker::versions_storage::versions_path;
use crate::id_tracker::point_mappings::PointMappings;

impl<S: UniversalRead> ReadOnlyAppendableIdTracker<S> {
    /// Open a read-only view over the appendable ID tracker data at `segment_path`, threading every
    /// file open through the filesystem handle `fs`.
    ///
    /// Both the mappings and versions files must already exist, this opens an existing appendable
    /// storage and never creates one. A missing file is an error rather than an empty tracker.
    ///
    /// Unlike [`MutableIdTracker::open`](crate::id_tracker::mutable_id_tracker::MutableIdTracker::open)
    /// this never writes to the storage. The initial state is loaded by running the same
    /// reconciliation as [`Self::live_reload`] from an empty tracker: the whole mappings log and
    /// versions file are consumed, applying only committed points (a partial trailing entry is
    /// simply not consumed and picked up on a later reload).
    pub fn open(
        fs: &S::Fs,
        segment_path: impl Into<PathBuf>,
        deferred_internal_id: Option<PointOffsetType>,
    ) -> OperationResult<Self> {
        let segment_path = segment_path.into();

        let mappings_path = mappings_path(&segment_path);
        let versions_path = versions_path(&segment_path);

        let options = OpenOptions {
            writeable: false,
            need_sequential: false,
            populate: Populate::No,
            advice: AdviceSetting::Advice(Normal),
        };

        let mappings_file = fs.open(mappings_path, options, Default::default())?;
        let versions_file = fs.open(versions_path, options, Default::default())?;

        let mut tracker = Self {
            segment_path,
            internal_to_version: Vec::new(),
            mappings: PointMappings::new(
                Default::default(),
                Default::default(),
                Default::default(),
                Default::default(),
                deferred_internal_id,
            ),
            pending_inserts: Default::default(),
            mappings_read_to: 0,
            mappings_file,
            versions_file,
        };

        // Load the existing data the same way a live-reload consumes appended data. The reported
        // delta (the whole committed set as inserts) is irrelevant for an initial open.
        tracker.live_reload()?;

        #[cfg(debug_assertions)]
        tracker.mappings.assert_mappings();

        Ok(tracker)
    }
}
