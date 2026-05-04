use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::is_alive_lock::IsAliveLock;
use crate::universal_io::{
    self, Flusher, OpenOptions, TypedStorage, UniversalRead, UniversalWrite,
};

/// A generic-storage wrapper for a type that should be possible to `read_whole` fairly cheaply.
///
/// It dereferences to a copy of the stored type in memory so that reads are fast.
/// On flush, the entire in-memory state is written to disk.
#[derive(Debug)]
pub struct StoredStruct<S, T> {
    inner: T,
    storage: Arc<Mutex<TypedStorage<S, T>>>,
    is_alive_lock: IsAliveLock,
}

impl<S, T> StoredStruct<S, T>
where
    T: bytemuck::Pod + Send,
    S: UniversalWrite<T> + Send + 'static,
{
    pub fn open(path: impl AsRef<Path>, options: OpenOptions) -> universal_io::Result<Self> {
        let storage = TypedStorage::open(path, options)?;
        let inner = storage.read_whole()?[0];
        Ok(Self {
            inner,
            storage: Arc::new(Mutex::new(storage)),
            is_alive_lock: IsAliveLock::new(),
        })
    }

    pub fn flusher(&self) -> Flusher {
        let state = self.inner; // copy current state
        let storage = Arc::downgrade(&self.storage);
        let is_alive = self.is_alive_lock.handle();

        Box::new(move || {
            let (Some(_is_alive), Some(storage)) = (is_alive.lock_if_alive(), storage.upgrade())
            else {
                // Storage was dropped before executing the flusher
                return Ok(());
            };

            let mut storage = storage.lock();
            storage.write(0, &[state])?;
            storage.flusher()()?;

            Ok(())
        })
    }
}

impl<S, T> std::ops::Deref for StoredStruct<S, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<S, T> std::ops::DerefMut for StoredStruct<S, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
