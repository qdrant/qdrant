//! Cross-platform positional file IO helpers, used by the append-only tracker.
//!
//! The append-only tracker reads and writes its file directly, without memory mapping it. The
//! append-only page goes through universal IO instead.

use std::io;
use std::path::Path;

use fs_err as fs;
use fs_err::File;

use crate::error::BlobstoreError;

/// Create a new empty file, truncating it if it already exists.
pub(crate) fn create_new(path: &Path) -> io::Result<File> {
    fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
}

/// Open an existing file, writable or read-only.
///
/// If the file does not exist, return an error mentioning `description`.
pub(crate) fn open_existing(
    path: &Path,
    writeable: bool,
    description: &str,
) -> crate::Result<File> {
    fs::OpenOptions::new()
        .read(true)
        .write(writeable)
        .open(path)
        .map_err(|err| {
            if err.kind() == io::ErrorKind::NotFound {
                // If config exists and this file doesn't,
                // it should be treated as inconsistent storage rather than a missing one
                BlobstoreError::service_error(format!(
                    "{description} file does not exist: {}",
                    path.display(),
                ))
            } else {
                BlobstoreError::from(err)
            }
        })
}

/// Read exactly `buf.len()` bytes from the file at the given byte offset.
#[cfg(unix)]
pub(crate) fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    use fs_err::os::unix::fs::FileExt as _;
    file.read_exact_at(buf, offset)
}

/// Write all bytes to the file at the given byte offset.
#[cfg(unix)]
pub(crate) fn write_all_at(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    use fs_err::os::unix::fs::FileExt as _;
    file.write_all_at(buf, offset)
}

/// Read exactly `buf.len()` bytes from the file at the given byte offset.
///
/// Note: unlike the unix variant, this moves the file cursor.
#[cfg(windows)]
pub(crate) fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt as _;
    while !buf.is_empty() {
        match file.file().seek_read(buf, offset) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ));
            }
            Ok(n) => {
                buf = &mut buf[n..];
                offset += n as u64;
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

/// Write all bytes to the file at the given byte offset.
///
/// Note: unlike the unix variant, this moves the file cursor.
#[cfg(windows)]
pub(crate) fn write_all_at(file: &File, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt as _;
    while !buf.is_empty() {
        match file.file().seek_write(buf, offset) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                ));
            }
            Ok(n) => {
                buf = &buf[n..];
                offset += n as u64;
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
    Ok(())
}
