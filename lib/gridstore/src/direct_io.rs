//! Cross-platform positional file IO helpers, used by the serverless storage mode.
//!
//! The serverless storage variant reads and writes its files directly, without memory mapping
//! them.

use std::io;

use fs_err::File;

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
