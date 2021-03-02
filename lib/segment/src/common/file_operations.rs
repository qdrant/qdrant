use std::fs::File;
use std::io::{Read, Write};
use crate::entry::entry_point::{OperationError, OperationResult};
use serde::Serialize;
use std::path::Path;
use atomicwrites::AtomicFile;
use atomicwrites::OverwriteBehavior::AllowOverwrite;
use serde::de::DeserializeOwned;

pub fn atomic_save_json<N: DeserializeOwned + Serialize>(path: &Path, object: &N) -> OperationResult<()> {
    let af = AtomicFile::new(path, AllowOverwrite);
    let state_bytes = serde_json::to_vec(object).unwrap();
    af.write(|f| {
        f.write_all(&state_bytes)
    })?;
    Ok(())
}

pub fn read_json<N: DeserializeOwned + Serialize>(path: &Path) -> OperationResult<N> {
    let mut contents = String::new();

    let mut file = File::open(path)?;
    file.read_to_string(&mut contents)?;

    let result: N = serde_json::from_str(&contents).or_else(|err| {
        Err(OperationError::ServiceError {
            description: format!("Failed to read data {}. Error: {}", path.to_str().unwrap(), err)
        })
    })?;

    Ok(result)
}