use std::fmt::Debug;

/// Per-call backend extras for [`UniversalReadFs::open`].
///
/// Marker trait for the value passed to
/// [`UniversalReadFs::open`](super::UniversalReadFs::open) as `extra`.
/// Implementers expose typed setters so generic-over-`Fs` callers can opt
/// into backend-specific knobs without naming the concrete extras type.
///
/// `Default` is required so callers can construct a neutral extras value
/// (e.g. `<Fs::OpenExtra>::default()`) and then chain typed setters.
pub trait OpenExtra: Default + Debug {
    /// Hint that the open should bypass the OS page cache. Backends that
    /// support it (e.g. `io_uring` via `O_DIRECT`) honor the flag; backends
    /// where it's meaningless (mmap, block-cache) treat this as a no-op.
    #[must_use]
    fn with_prevent_caching(self, prevent_caching: bool) -> Self;

    /// Hint about the length of the file.
    fn with_known_len(self, known_len: u64) -> Self;
}

impl OpenExtra for () {
    fn with_prevent_caching(self, _prevent_caching: bool) -> Self {}

    fn with_known_len(self, _known_len: u64) -> Self {}
}
