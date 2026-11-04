/// Whether [`super::segment_builder::SegmentBuilder::build`] drops the page cache of the on-disk
/// components it built: vector storages and indexes, quantized vectors, payload storage and
/// index, id tracker.
///
/// `Keep` leaves them cached, for a caller that reads the built segment right away and then
/// deletes it, which frees the cache with the files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheAfterBuild {
    Drop,
    Keep,
}

impl CacheAfterBuild {
    pub fn is_drop(self) -> bool {
        match self {
            Self::Drop => true,
            Self::Keep => false,
        }
    }
}
