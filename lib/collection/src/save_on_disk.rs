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

    pub fn write(&mut self) -> WriteGuard<T> {
        WriteGuard(self)
    }
}

impl<T> Deref for SaveOnDisk<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<'a, T: Serialize> WriteGuard<'a, T> {
    pub fn save(&self) -> Result<(), Error> {
        AtomicFile::new(&self.0.path, AllowOverwrite).write(|file| {
            let writer = BufWriter::new(file);
            serde_json::to_writer(writer, &self.0.data)
        })?;
        Ok(())
    }
}

impl<'a, T: Serialize> Deref for WriteGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<'a, T: Serialize> DerefMut for WriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0.data
    }
}

impl<'a, T: Serialize> Drop for WriteGuard<'a, T> {
    fn drop(&mut self) {
        if let Err(err) = self.save() {
            log::error!(
                "Failed to save structure on disk at {} with error: {err}",
                self.0.path.display()
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::SaveOnDisk;

    #[test]
    fn saves_data() {
        let dir = tempdir::TempDir::new("test").unwrap();
        let counter_file = dir.path().join("counter");
        let mut counter: SaveOnDisk<u32> = SaveOnDisk::load_or_init(&counter_file).unwrap();
        {
            let mut counter_guard1 = counter.write();
            *counter_guard1 += 1;
        }
        assert_eq!(*counter, 1);
        assert_eq!(
            counter.to_string(),
            fs::read_to_string(&counter_file).unwrap()
        );
        {
            let mut counter_guard2 = counter.write();
            *counter_guard2 += 1;
        }
        assert_eq!(*counter, 2);
        assert_eq!(
            counter.to_string(),
            fs::read_to_string(&counter_file).unwrap()
        );
    }

    #[test]
    fn loads_data() {
        let dir = tempdir::TempDir::new("test").unwrap();
        let counter_file = dir.path().join("counter");
        {
            let mut counter: SaveOnDisk<u32> = SaveOnDisk::load_or_init(&counter_file).unwrap();
            let mut counter_guard = counter.write();
            *counter_guard += 1;
        }
        let counter: SaveOnDisk<u32> = SaveOnDisk::load_or_init(&counter_file).unwrap();
        assert_eq!(*counter, 1)
    }
}
