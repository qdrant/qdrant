use std::path::PathBuf;

use common::universal_io::UniversalRead;

use crate::common::flags::read_only_roaring_flags::ReadOnlyRoaringFlags;

mod read_ops;

/// Read-only counterpart of [`MutableNullIndex`][1] / [`ImmutableNullIndex`][2].
///
/// All flags are loaded into in-memory roaring bitmaps on open. The backing
/// storage is bound to [`UniversalRead`] only — no buffer, no flusher,
/// no write path. Query logic (filter / cardinality / condition checker) is
/// shared with the writable variant via the [`NullIndexRead`][3] trait.
///
/// [1]: super::mutable_null_index::MutableNullIndex
/// [2]: super::immutable_null_index::ImmutableNullIndex
/// [3]: super::read_ops::NullIndexRead
pub struct ReadOnlyNullIndex<S: UniversalRead> {
    #[allow(dead_code)]
    pub(super) _base_dir: PathBuf,
    pub(super) storage: ReadOnlyStorage<S>,
    pub(super) total_point_count: usize,
}

pub(super) struct ReadOnlyStorage<S: UniversalRead> {
    /// Points which have at least one value
    pub(super) has_values_flags: ReadOnlyRoaringFlags<S>,
    /// Points which have null values
    pub(super) is_null_flags: ReadOnlyRoaringFlags<S>,
}
