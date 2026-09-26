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

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;

    const HEADER_SIZE: usize = 512;
    const FILE_SIZE: usize = 3000;

    /// Builds an archive with one regular file of `FILE_SIZE` bytes, spanning multiple blocks.
    fn build_archive() -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        let mut header = tar::Header::new_gnu();
        header.set_size(FILE_SIZE as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder
            .append_data(&mut header, "dir/file.bin", vec![7u8; FILE_SIZE].as_slice())
            .unwrap();
        builder.into_inner().unwrap()
    }

    /// Error that, like HTTP client errors, describes what happened only in its source.
    #[derive(Debug, thiserror::Error)]
    #[error("error decoding response body")]
    struct BodyError(#[source] io::Error);

    /// Reader that fails like an HTTP client whose connection got reset.
    #[derive(Debug)]
    struct FailingReader;

    impl Read for FailingReader {
        fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                BodyError(io::Error::other("connection reset by peer")),
            ))
        }
    }

    #[test]
    fn unpacks_complete_archive() {
        let dst = tempfile::tempdir().unwrap();

        tar_unpack_reader(build_archive().as_slice(), dst.path()).unwrap();

        let metadata = fs::metadata(dst.path().join("dir/file.bin")).unwrap();
        assert_eq!(metadata.len(), FILE_SIZE as u64);
    }

    /// The stream ends before the archive is complete, like when the source of a streamed
    /// snapshot fails halfway and ends the response.
    #[test]
    fn truncated_archive_reports_closed_stream() {
        let archive = build_archive();

        // Cut mid-header, right after the header and mid-file
        for cut in [300, HEADER_SIZE, HEADER_SIZE + 1000] {
            let dst = tempfile::tempdir().unwrap();

            let err = tar_unpack_reader(&archive[..cut], dst.path()).unwrap_err();

            assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof, "cut at {cut}");
            assert_eq!(
                err.to_string(),
                "Connection or stream closed before tar archive was complete",
                "cut at {cut}",
            );
        }
    }

    /// The reader fails, like when the connection of a streamed snapshot is killed. The error
    /// kind and the chain of sources describing the cause are reported.
    #[test]
    fn failing_reader_reports_closed_connection() {
        let dst = tempfile::tempdir().unwrap();
        let archive = build_archive();
        let reader = archive[..HEADER_SIZE + 1000].chain(FailingReader);

        let err = tar_unpack_reader(reader, dst.path()).unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::ConnectionReset);
        assert_eq!(
            err.to_string(),
            "Connection or stream closed while reading tar archive: \
             error decoding response body: connection reset by peer",
        );
    }

    /// A malformed archive that is read in full is still reported as malformed.
    #[test]
    fn malformed_archive_reports_malformed_archive() {
        let dst = tempfile::tempdir().unwrap();
        let garbage = vec![0xFF; 2 * HEADER_SIZE];

        let err = tar_unpack_reader(garbage.as_slice(), dst.path()).unwrap_err();

        assert!(
            err.to_string()
                .starts_with("Malformed tar archive, reached unknown entry"),
            "{err}",
        );
    }
}
