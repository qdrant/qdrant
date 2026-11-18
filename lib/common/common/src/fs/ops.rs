use std::io::{BufReader, BufWriter, Error, Write};
use std::path::Path;

use fs_err::File;
use serde::Serialize;
use serde::de::DeserializeOwned;

type Result<T, E = Error> = std::result::Result<T, E>;

/// Same as `atomicwrites::AtomicFile::write`, but retries the final replace, see
/// [`rename_while_held`](super::rename_while_held).
#[allow(
    clippy::disallowed_types,
    reason = "`write` takes `std::fs::File` for compatibility with `atomicwrites`"
)]
pub fn atomic_save<E, F>(path: &Path, write: F) -> Result<(), E>
where
    E: From<Error>,
    F: FnOnce(&mut BufWriter<&mut std::fs::File>) -> Result<(), E>,
{
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        Some(_) | None => Path::new("."),
    };
    let tmp_dir = tempfile::Builder::new()
        .prefix(".atomicwrite")
        .tempdir_in(parent)?;
    let tmp_path = tmp_dir.path().join("tmpfile.tmp");

    let mut file = File::create(&tmp_path)?;
    let mut writer = BufWriter::new(file.file_mut());
    write(&mut writer)?;
    writer.flush().map_err(|e| {
        E::from(Error::new(
            e.kind(),
            format!("Failed to flush {}: {e}", path.display()),
        ))
    })?;
    drop(writer);
    file.sync_all()?;
    drop(file);

    super::retry::retry_while_held(path, || atomicwrites::replace_atomic(&tmp_path, path))?;
    Ok(())
}

pub fn atomic_save_bin<T: Serialize>(path: &Path, object: &T) -> Result<()> {
    atomic_save(path, |writer| {
        bincode::serialize_into(writer, object)
            .map_err(|e| Error::other(format!("Failed to serialize {}: {e}", path.display())))
    })
}

pub fn atomic_save_json<T: Serialize>(path: &Path, object: &T) -> Result<()> {
    atomic_save(path, |writer| {
        serde_json::to_writer(writer, object)
            .map_err(|e| Error::other(format!("Failed to serialize {}: {e}", path.display())))
    })
}

pub fn read_bin<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let file = File::open(path)?;
    bincode::deserialize_from(BufReader::new(file))
        .map_err(|e| Error::other(format!("Failed to deserialize {}: {e}", path.display())))
}

pub fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let file = File::open(path)?;
    serde_json::from_reader(BufReader::new(file))
        .map_err(|e| Error::other(format!("Failed to deserialize {}: {e}", path.display())))
}
