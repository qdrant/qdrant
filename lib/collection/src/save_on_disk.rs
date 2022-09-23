use std::fs::File;
use std::io::BufWriter;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;

use atomicwrites::OverwriteBehavior::AllowOverwrite;
use atomicwrites::{AtomicFile, Error as AtomicWriteError};
use serde::{Deserialize, Serialize};

/// Functions as a smart pointer which gives a write guard and saves data on disk
/// when write guard is dropped.
#[derive(Clone, Debug, Default)]
pub struct SaveOnDisk<T> {
    data: T,
    path: PathBuf,
}

pub struct WriteGuard<'a, T: Serialize>(&'a mut SaveOnDisk<T>);

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to save structure on disk with error: {0}")]
    AtomicWrite(#[from] AtomicWriteError<serde_json::Error>),
    #[error("Failed to perform io operation: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Failed to (de)serialize from/to json: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("Error in write closure: {0}")]
    FromClosure(Box<dyn std::error::Error>),
}

impl<T: Serialize + Default + for<'de> Deserialize<'de>> SaveOnDisk<T> {
    pub fn load_or_init(path: impl Into<PathBuf>) -> Result<Self, Error> {
        let path: PathBuf = path.into();
        let data = if path.exists() {
            let file = File::open(&path)?;
            serde_json::from_reader(&file)?
        } else {
            Default::default()
        };
        Ok(Self { data, path })
    }

    pub fn write_with_res<O, E: std::error::Error + 'static>(
        &mut self,
        f: impl FnOnce(&mut T) -> Result<O, E>,
    ) -> Result<O, Error> {
        let output = f(&mut self.data).map_err(|err| Error::FromClosure(Box::new(err)))?;
        self.save()?;
        Ok(output)
    }

    pub fn write<O>(&mut self, f: impl FnOnce(&mut T) -> O) -> Result<O, Error> {
        let output = f(&mut self.data);
        self.save()?;
        Ok(output)
    }

    pub fn save(&self) -> Result<(), Error> {
        AtomicFile::new(&self.path, AllowOverwrite).write(|file| {
            let writer = BufWriter::new(file);
            serde_json::to_writer(writer, &self.data)
        })?;
        Ok(())
    }
}

impl<T> Deref for SaveOnDisk<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T> DerefMut for SaveOnDisk<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::Builder;

    use super::SaveOnDisk;

    #[test]
    fn saves_data() {
        let dir = Builder::new().prefix("test").tempdir().unwrap();
        let counter_file = dir.path().join("counter");
        let mut counter: SaveOnDisk<u32> = SaveOnDisk::load_or_init(&counter_file).unwrap();
        counter.write(|counter| *counter += 1).unwrap();
        assert_eq!(*counter, 1);
        assert_eq!(
            counter.to_string(),
            fs::read_to_string(&counter_file).unwrap()
        );
        counter.write(|counter| *counter += 1).unwrap();
        assert_eq!(*counter, 2);
        assert_eq!(
            counter.to_string(),
            fs::read_to_string(&counter_file).unwrap()
        );
    }

    #[test]
    fn loads_data() {
        let dir = Builder::new().prefix("test").tempdir().unwrap();
        let counter_file = dir.path().join("counter");
        let mut counter: SaveOnDisk<u32> = SaveOnDisk::load_or_init(&counter_file).unwrap();
        counter.write(|counter| *counter += 1).unwrap();
        let counter: SaveOnDisk<u32> = SaveOnDisk::load_or_init(&counter_file).unwrap();
        assert_eq!(*counter, 1)
    }
}
