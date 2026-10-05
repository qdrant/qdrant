//! Wrappers around [`tar::Archive::unpack()`] with extra safety checks.

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
    fs::create_dir_all(dst)?;
    let dst = &fs::canonicalize(dst).unwrap_or(dst.to_path_buf());

    let mut archive = Archive::new(reader);
    archive.set_overwrite(false);

    let entries = archive
        .entries()
        .map_err(|err| tar_error("unable to read entries", err))?;

    for entry in entries {
        let mut entry = entry.map_err(|err| tar_error("reached unknown entry", err))?;

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

    Ok(archive.into_inner())
}

fn tar_error(message: &str, source: io::Error) -> io::Error {
    // Hide error in release builds to not leak contents of potentially sensitive files
    let message = if cfg!(debug_assertions) {
        format!("Malformed tar archive, {message}: {source}")
    } else {
        format!("Malformed tar archive, {message}")
    };

    io::Error::new(source.kind(), message)
}
