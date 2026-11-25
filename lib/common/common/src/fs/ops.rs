use std::io::{BufReader, BufWriter, Error, Write};
use std::path::Path;

#[cfg(not(windows))]
use atomicwrites::{AtomicFile, OverwriteBehavior};
use fs_err::File;
use serde::Serialize;
use serde::de::DeserializeOwned;

type Result<T, E = Error> = std::result::Result<T, E>;

#[allow(
    clippy::disallowed_types,
    reason = "can't use `fs_err::File` since `atomicwrites` only provides `&mut std::fs::File`"
)]
pub fn atomic_save<E, F>(path: &Path, write: F) -> Result<(), E>
where
    E: From<Error>,
    F: FnOnce(&mut BufWriter<&mut std::fs::File>) -> Result<(), E>,
{
    #[cfg(windows)]
    {
        atomic_save_windows(path, write)
    }
    #[cfg(not(windows))]
    {
        let af = AtomicFile::new(path, OverwriteBehavior::AllowOverwrite);
        af.write(|f| write_and_flush(path, f, write))
            .map_err(|e| match e {
                atomicwrites::Error::Internal(err) => E::from(err),
                atomicwrites::Error::User(err) => err,
            })
    }
}

/// Same as `atomicwrites` (temp file in the same directory, then replace),
/// but retries the final rename, which an antivirus/EDR scanner holding the
/// target or the fresh temp file can make fail with access denied.
/// Persistence retains `atomicwrites`' Windows write-through replacement.
#[cfg(windows)]
#[allow(
    clippy::disallowed_types,
    clippy::disallowed_methods,
    reason = "`tempfile` provides `std::fs::File`, see `atomic_save`"
)]
fn atomic_save_windows<E, F>(path: &Path, write: F) -> Result<(), E>
where
    E: From<Error>,
    F: FnOnce(&mut BufWriter<&mut std::fs::File>) -> Result<(), E>,
{
    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    let mut tmp = tempfile::Builder::new()
        .prefix(".atomicwrite")
        .tempfile_in(parent)
        .map_err(E::from)?;
    write_and_flush(path, tmp.as_file_mut(), write)?;
    tmp.as_file().sync_all().map_err(E::from)?;
    // Closes the file handle before renaming
    let temp_path = tmp.into_temp_path();
    super::persist_with_retry(temp_path, path).map_err(E::from)
}

#[allow(clippy::disallowed_types, reason = "see `atomic_save`")]
fn write_and_flush<E, F>(path: &Path, file: &mut std::fs::File, write: F) -> Result<(), E>
where
    E: From<Error>,
    F: FnOnce(&mut BufWriter<&mut std::fs::File>) -> Result<(), E>,
{
    let mut writer = BufWriter::new(file);
    write(&mut writer)?;
    writer.flush().map_err(|e| {
        E::from(Error::new(
            e.kind(),
            format!("Failed to flush {}: {e}", path.display()),
        ))
    })?;
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
