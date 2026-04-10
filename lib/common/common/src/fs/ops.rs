use std::io::{BufReader, BufWriter, Error, Write};
use std::path::Path;

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
    let af = AtomicFile::new(path, OverwriteBehavior::AllowOverwrite);
    af.write(|f| {
        let mut writer = BufWriter::new(f);
        write(&mut writer)?;
        writer.flush().map_err(|e| {
            E::from(Error::new(
                e.kind(),
                format!("Failed to flush {}: {e}", path.display()),
            ))
        })?;
        Ok(())
    })
    .map_err(|e| match e {
        atomicwrites::Error::Internal(err) => E::from(err),
        atomicwrites::Error::User(err) => err,
    })
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
