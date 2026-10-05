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
///
/// Errors caused by the archive content itself (malformed data or forbidden
/// entry types) use [`io::ErrorKind::InvalidData`], so callers handling
/// user-supplied archives can tell them apart from local IO failures.
pub fn tar_unpack_reader<R: io::Read>(reader: R, dst: &Path) -> Result<R, io::Error> {
    let mut archive = Archive::new(reader);
    archive.set_overwrite(false);

    fs::create_dir_all(dst)?;
    let dst = &fs::canonicalize(dst).unwrap_or(dst.to_path_buf());

    for entry in archive.entries().map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            // Must hide error in release builds to not leak contents of potentially sensitive files
            #[cfg(not(debug_assertions))]
            format!("Malformed tar archive, unable to read entries"),
            #[cfg(debug_assertions)]
            format!("Malformed tar archive, unable to read entries: {err}"),
        )
    })? {
        let mut entry = entry.map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
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
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Forbidden entry type in tar archive: {entry_type:?}"),
                ));
            }
        }
        entry.unpack_in(dst)?;
    }

    Ok(archive.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unpack_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir()
            .join(format!("tar-unpack-test-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn malformed_archive_is_invalid_data() {
        let garbage: Vec<u8> = (0..10_000u32).map(|i| (i % 251) as u8).collect();
        let dir = unpack_dir("garbage");
        let err = match tar_unpack_reader(io::BufReader::new(&garbage[..]), &dir) {
            Ok(_) => panic!("malformed archive should fail to unpack"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn forbidden_entry_type_is_invalid_data() {
        let mut buf = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut buf);
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Symlink);
            header.set_size(0);
            header.set_cksum();
            builder
                .append_data(&mut header, "evil-link", io::empty())
                .unwrap();
            builder.finish().unwrap();
        }
        let dir = unpack_dir("symlink");
        let err = match tar_unpack_reader(io::BufReader::new(&buf[..]), &dir) {
            Ok(_) => panic!("archive with a symlink entry should fail to unpack"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn valid_archive_still_extracts() {
        let mut buf = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut buf);
            let content = b"hello world";
            let mut header = tar::Header::new_gnu();
            header.set_size(content.len() as u64);
            header.set_cksum();
            builder
                .append_data(&mut header, "config.json", &content[..])
                .unwrap();
            builder.finish().unwrap();
        }
        let dir = unpack_dir("valid");
        tar_unpack_reader(io::BufReader::new(&buf[..]), &dir).unwrap();
        assert_eq!(
            std::fs::read(dir.join("config.json")).unwrap(),
            b"hello world"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
