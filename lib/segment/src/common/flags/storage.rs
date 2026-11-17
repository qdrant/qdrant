use std::path::PathBuf;

use common::types::PointOffsetType;
use common::universal_io::{UniversalRead, UniversalWrite};

use super::buffered_dynamic_flags::BufferedDynamicFlags;
use super::compact_stored_flags::CompactStoredFlags;
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;

/// Write side of a flags stack, dispatching between the storage modes.
///
/// The in-memory read state lives in the wrappers ([`BitvecFlags`] /
/// [`RoaringFlags`]); this enum only persists their mutations, in the flavor
/// of the [`FlagsMode`](super::FlagsMode) the flags were created in.
///
/// [`BitvecFlags`]: super::bitvec_flags::BitvecFlags
/// [`RoaringFlags`]: super::roaring_flags::RoaringFlags
#[derive(Debug)]
pub(crate) enum FlagsStorage<S: UniversalRead> {
    /// Mmapped files mutated in place, with changes buffered until flush.
    Dynamic(BufferedDynamicFlags<S>),

    /// RAM-resident compact bitmask, whole-file rewrite on flush.
    Compact(CompactStoredFlags<S>),
}

impl<S> FlagsStorage<S>
where
    S: UniversalWrite + Send + 'static,
    S::Fs: Send + Sync + 'static,
{
    /// Record setting the flag at `index`, to be persisted on the next flush.
    pub fn set(&self, index: PointOffsetType, value: bool) {
        match self {
            Self::Dynamic(storage) => storage.buffer_set(index, value),
            Self::Compact(storage) => {
                storage.set(index, value);
            }
        }
    }

    pub fn files(&self) -> Vec<PathBuf> {
        match self {
            Self::Dynamic(storage) => storage.files(),
            Self::Compact(storage) => storage.files(),
        }
    }

    pub fn flusher(&self) -> Flusher {
        match self {
            Self::Dynamic(storage) => storage.flusher(),
            Self::Compact(storage) => storage.flusher(),
        }
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        match self {
            Self::Dynamic(storage) => storage.clear_cache(),
            Self::Compact(storage) => storage.clear_cache(),
        }
    }
}
