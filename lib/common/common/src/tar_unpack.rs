//! Wrappers around [`tar::Archive::unpack()`] with extra safety checks.

use std::error::Error;
use std::io;
use std::path::Path;

use fs_err as fs;
use tar::{Archive, EntryType};

pub fn tar_unpack_file(path: &Path, dst: &Path) -> Result<(), io::Error> {
    let reader = io::BufReader::new(fs::File::open(path)?);
    tar_unpack_reader(reader, dst)?;
    Ok(())
}

/// Same as [`Archive::new()`] followed by [`Archive::unpack()`], but checks
/// that we don't unpack something beyond regular files and directories.
///
/// Accepts a reader and returns the same reader.
pub fn tar_unpack_reader<R: io::Read>(reader: R, dst: &Path) -> Result<R, io::Error> {
    let mut archive = Archive::new(TrackedReader {
        inner: reader,
        ended: None,
    });
    archive.set_overwrite(false);

    fs::create_dir_all(dst)?;
    let dst = &fs::canonicalize(dst).unwrap_or(dst.to_path_buf());

    let result = unpack_entries(&mut archive, dst);
    let reader = archive.into_inner();

    match result {
        Ok(()) => Ok(reader.inner),
        // The `tar` crate reports a failing reader, or a stream that ends halfway through an
        // entry, as a generic archive error. Report what happened to the reader instead.
        Err(err) => Err(reader.ended.unwrap_or(err)),
    }
}

fn unpack_entries<R: io::Read>(archive: &mut Archive<R>, dst: &Path) -> Result<(), io::Error> {
    for entry in archive.entries().map_err(|err| {
        io::Error::new(
            err.kind(),
            // Must hide error in release builds to not leak contents of potentially sensitive files
            #[cfg(not(debug_assertions))]
            format!("Malformed tar archive, unable to read entries"),
            #[cfg(debug_assertions)]
            format!("Malformed tar archive, unable to read entries: {err}"),
        )
    })? {
        let mut entry = entry.map_err(|err| {
            io::Error::new(
                err.kind(),
                // Must hide error in release builds to not leak contents of potentially sensitive files
                #[cfg(not(debug_assertions))]
                format!("Malformed tar archive, reached unknown entry"),
                #[cfg(debug_assertions)]
                format!("Malformed tar archive, reached unknown entry: {err}"),
            )
        })?;

        #[expect(clippy::wildcard_enum_match_arm, reason = "#[non_exhaustive] enum")]
        match entry.header().entry_type() {
            EntryType::Directory | EntryType::Regular | EntryType::GNUSparse => (),
            entry_type => {
                return Err(io::Error::other(format!(
                    "Forbidden entry type in tar archive: {entry_type:?}"
                )));
            }
        }
        entry.unpack_in(dst)?;
    }

    Ok(())
}

/// Reader that remembers the stream failing or ending, to report that instead of a malformed
/// archive if unpacking then fails.
struct TrackedReader<R> {
    inner: R,
    ended: Option<io::Error>,
}

impl<R: io::Read> io::Read for TrackedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let result = self.inner.read(buf);
        match &result {
            Ok(0) if !buf.is_empty() => {
                self.ended = Some(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Connection or stream closed before tar archive was complete",
                ));
            }
            Err(err) => {
                // HTTP client errors describe what happened, such as the connection being
                // closed, only in their source
                let cause = std::iter::successors(Some(err as &dyn Error), |&err| err.source())
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(": ");
                self.ended = Some(io::Error::new(
                    err.kind(),
                    format!("Connection or stream closed while reading tar archive: {cause}"),
                ));
            }
            Ok(_) => {}
        }
        result
    }
}
