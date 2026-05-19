//! Process-global tokio handle registry.
//!
//! The bridge needs a runtime to dispatch async work into. Each [`IoBridge`]
//! adapter takes an explicit `Handle` in [`IoBridge::new`], which is the
//! preferred path. But [`UniversalRead::open`] is an associated function —
//! there's no `&self` to pull a handle out of. To make `open` viable through
//! the bridge, the application installs a process-global handle once at
//! startup and the bridge falls back to it.
//!
//! [`IoBridge`]: crate::IoBridge
//! [`IoBridge::new`]: crate::IoBridge::new
//! [`UniversalRead::open`]: common::universal_io::UniversalRead::open
//!
//! Resolution order in [`resolve_handle`]:
//! 1. `Handle::try_current()` — the caller is already inside a tokio runtime.
//! 2. The handle installed via [`set_global_async_handle`].
//! 3. Error: `Uninitialized`.

use std::sync::OnceLock;

use common::universal_io::{Result, UniversalIoError};
use tokio::runtime::Handle;

static GLOBAL_HANDLE: OnceLock<Handle> = OnceLock::new();

/// Install a process-global tokio handle. Idempotent: the first call wins,
/// subsequent calls return the handle they were given back as `Err`.
pub fn set_global_async_handle(handle: Handle) -> std::result::Result<(), Handle> {
    GLOBAL_HANDLE.set(handle)
}

/// Snapshot the currently-installed global handle, if any.
pub fn global_async_handle() -> Option<Handle> {
    GLOBAL_HANDLE.get().cloned()
}

/// Resolve a tokio handle for bridge use. Prefers the ambient runtime (set
/// when running inside a tokio context), then falls back to the global one
/// installed by [`set_global_async_handle`]. Errors out as `Uninitialized` if
/// neither is available.
pub(crate) fn resolve_handle() -> Result<Handle> {
    Handle::try_current()
        .ok()
        .or_else(global_async_handle)
        .ok_or_else(|| {
            UniversalIoError::uninitialized(
                "IoBridge: no tokio runtime available — install one via \
                 `set_global_async_handle` or invoke from inside a \
                 tokio context",
            )
        })
}
