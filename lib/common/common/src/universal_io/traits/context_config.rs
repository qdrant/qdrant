/// Per-backend open-time context, with universal builder methods.
///
/// Generic-over-`S` callers cannot reach into a backend's concrete
/// [`UniversalReadFileOps::ContextConfig`](super::UniversalReadFileOps::ContextConfig)
/// to set cross-backend knobs. Instead, those knobs are exposed as builder
/// methods on this trait. Backends that honor a knob implement the method
/// substantively; backends that don't simply return `self` unchanged. The
/// blanket impl for `()` below is the canonical "ignores every knob"
/// implementation, used by backends with no per-call config (e.g. `MmapFile`).
pub trait TConfigContext: Default {
    /// Request that the backend bypass the OS page cache for this file (e.g.
    /// `io_uring` honors via `O_DIRECT`; mmap-based backends cannot bypass and
    /// must implement as a no-op).
    fn with_prevent_caching(self, prevent_caching: bool) -> Self;
}

impl TConfigContext for () {
    fn with_prevent_caching(self, _prevent_caching: bool) -> Self {
        self
    }
}
