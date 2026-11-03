/// Whether [`super::segment_builder::SegmentBuilder::build`] drops the page cache of the payload
/// storage and payload index it built, when they are on disk.
///
/// `Keep` leaves them cached for the caller to read the built segment's payload cheaply. The
/// caller then owns dropping the cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadCacheAfterBuild {
    Drop,
    Keep,
}
