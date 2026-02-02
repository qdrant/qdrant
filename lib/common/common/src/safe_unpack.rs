use std::io;
use std::path::Path;

use tar::Archive;

/// Same as tar::Archive::unpack but checks that we don't unpack something
/// beyond regular files and directories
pub fn safe_unpack<R: std::io::Read>(
    mut archive: Archive<R>,
    target_dir: &Path,
) -> Result<Archive<R>, std::io::Error> {
    fs_err::create_dir_all(target_dir)?;
    let dst = &fs_err::canonicalize(target_dir).unwrap_or(target_dir.to_path_buf());

    for entry in archive.entries()? {
        let mut entry = entry?;

        match entry.header().entry_type() {
            tar::EntryType::Directory | tar::EntryType::Regular | tar::EntryType::GNUSparse => (),
            entry_type => {
                return Err(std::io::Error::other(format!(
                    "Invalid entry type in tar archive: {entry_type:?}"
                )));
            }
        }
        entry.unpack_in(dst)?;
    }

    Ok(archive)
}

pub fn open_snapshot_archive(
    path: &Path,
) -> Result<tar::Archive<impl io::Read + io::Seek>, io::Error> {
    let file = fs_err::File::open(path)?;

    let mut ar = tar::Archive::new(io::BufReader::new(file));
    ar.set_overwrite(false);

    Ok(ar)
}
