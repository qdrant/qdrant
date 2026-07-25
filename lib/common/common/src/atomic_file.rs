//! Atomic file writes.
//!
//! A re-export of the [`atomicwrites`] crate on targets it supports, plus an API-compatible
//! in-place fallback for wasm32, which `atomicwrites` does not build for (it needs a `unix` or
//! `windows` backend for the rename-into-place step).
//!
//! The fallback writes straight to the destination, so it is *not* atomic. That is sound where it
//! is used: wasm32 has no filesystem of its own, so every write through it fails at the
//! `File::create` call anyway, and the read-only path this target exists for never writes.

#[cfg(not(target_arch = "wasm32"))]
pub use atomicwrites::{AllowOverwrite, AtomicFile, Error, OverwriteBehavior};
#[cfg(target_arch = "wasm32")]
pub use wasm::{AllowOverwrite, AtomicFile, Error, OverwriteBehavior};

#[cfg(target_arch = "wasm32")]
mod wasm {
    use std::error::Error as ErrorTrait;
    use std::path::{Path, PathBuf};
    use std::{fmt, fs, io};

    pub use OverwriteBehavior::AllowOverwrite;

    /// Whether to allow overwriting if the target file exists.
    #[derive(Clone, Copy)]
    pub enum OverwriteBehavior {
        /// Overwrite files silently.
        AllowOverwrite,

        /// Don't overwrite files.
        DisallowOverwrite,
    }

    #[derive(Debug)]
    pub enum Error<E> {
        /// The error originated in this module, while opening the destination.
        Internal(io::Error),
        /// The error originated in the user-supplied callback.
        User(E),
    }

    impl From<Error<io::Error>> for io::Error {
        fn from(e: Error<io::Error>) -> Self {
            match e {
                Error::Internal(x) | Error::User(x) => x,
            }
        }
    }

    impl<E: fmt::Display> fmt::Display for Error<E> {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            match self {
                Error::Internal(e) => e.fmt(f),
                Error::User(e) => e.fmt(f),
            }
        }
    }

    impl<E: ErrorTrait + 'static> ErrorTrait for Error<E> {
        fn source(&self) -> Option<&(dyn ErrorTrait + 'static)> {
            match self {
                Error::Internal(e) => Some(e),
                Error::User(e) => Some(e),
            }
        }
    }

    pub struct AtomicFile {
        path: PathBuf,
        #[expect(
            dead_code,
            reason = "Kept for API parity with `atomicwrites::AtomicFile`"
        )]
        overwrite: OverwriteBehavior,
    }

    impl AtomicFile {
        pub fn new<P: AsRef<Path>>(path: P, overwrite: OverwriteBehavior) -> Self {
            AtomicFile {
                path: path.as_ref().to_owned(),
                overwrite,
            }
        }

        pub fn path(&self) -> &Path {
            &self.path
        }

        pub fn write<T, E, F>(&self, f: F) -> Result<T, Error<E>>
        where
            F: FnOnce(&mut fs::File) -> Result<T, E>,
        {
            #[expect(
                clippy::disallowed_types,
                reason = "Mirrors the `&mut std::fs::File` callback of `atomicwrites::AtomicFile`"
            )]
            let mut file = fs::File::create(&self.path).map_err(Error::Internal)?;
            f(&mut file).map_err(Error::User)
        }
    }
}
