/// Per-backend construction context.
///
/// Marker trait for the value passed to
/// [`UniversalReadFileOps::from_context`](super::UniversalReadFileOps::from_context)
/// when building a filesystem handle. The `Default` requirement lets
/// generic-over-`Fs` callers (and tests) build a backend with no extra
/// configuration via `<Fs::ContextConfig>::default()`.
///
/// Per-call knobs (e.g. `prevent_caching`) live on
/// [`OpenOptions`](super::super::OpenOptions), not here.
pub trait TConfigContext: Default {}

impl TConfigContext for () {}
