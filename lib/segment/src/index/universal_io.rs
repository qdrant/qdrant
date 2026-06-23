use common::universal_io::UniversalRead;

/// Extensions over [`UniversalRead`].
///
/// Sometimes generic-over-[`UniversalRead`] code must branch on the concrete
/// UIO type. This trait is a place for such branching. Any implementations of
/// [`UniversalRead`] used in this [crate] should implement this.
pub trait UniversalReadExt: UniversalRead {}

impl<T: UniversalRead> UniversalReadExt for T {}
