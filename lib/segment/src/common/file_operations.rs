use crate::entry::entry_point::{OperationError, OperationResult};
use atomicwrites::AtomicFile;
use atomicwrites::OverwriteBehavior::AllowOverwrite;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::Path;

pub fn atomic_save_bin<N: DeserializeOwned + Serialize>(
    path: &Path,
    object: &N,
) -> OperationResult<()> {
    let af = AtomicFile::new(path, AllowOverwrite);
    af.write(|f| {
        let mut writer = BufWriter::new(f);
        bincode::serialize_into(&mut writer, object)
    })?;
    Ok(())
}

pub fn atomic_save_json<N: DeserializeOwned + Serialize>(
    path: &Path,
    object: &N,
) -> OperationResult<()> {
    let af = AtomicFile::new(path, AllowOverwrite);
    let state_bytes = serde_json::to_vec(object).unwrap();
    af.write(|f| f.write_all(&state_bytes))?;
    Ok(())
}

pub fn read_json<N: DeserializeOwned + Serialize>(path: &Path) -> OperationResult<N> {
    let mut contents = String::new();

    let mut file = File::open(path)?;
    file.read_to_string(&mut contents)?;

    let result: N = serde_json::from_str(&contents).map_err(|err| {
        OperationError::service_error(&format!(
            "Failed to read data {}. Error: {}",
            path.to_str().unwrap(),
            err
        ))
    })?;

    Ok(result)
}

pub fn read_bin<N: DeserializeOwned + Serialize>(path: &Path) -> OperationResult<N> {
    let mut file = File::open(path)?;

    let result: N = bincode::deserialize_from(&mut file).map_err(|err| {
        OperationError::service_error(&format!(
            "Failed to read data {}. Error: {}",
            path.to_str().unwrap(),
            err
        ))
    })?;

    Ok(result)
}
