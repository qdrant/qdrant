use std::borrow::Cow;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use common::generic_consts::AccessPattern;
use common::universal_io::{
    BorrowedReadPipeline, OwnedReadPipeline, ReadRange, Result, UniversalIoError, UserData,
};
use object_store::ObjectStore;
use tokio::sync::mpsc;

use crate::file::S3File;
use crate::runtime::{S3Request, S3Response, S3RuntimeHandle};

pub(crate) const S3_PIPELINE_CAPACITY: usize = 256;

pub(crate) struct S3PipelineInner<U> {
    runtime: S3RuntimeHandle,
    tx: mpsc::Sender<S3Response>,
    rx: mpsc::Receiver<S3Response>,
    pending: HashMap<u64, U>,
    next_slot: u64,
}

impl<U> S3PipelineInner<U>
where
    U: UserData,
{
    pub(crate) fn new(runtime: S3RuntimeHandle) -> Self {
        let (tx, rx) = mpsc::channel(S3_PIPELINE_CAPACITY);
        Self {
            runtime,
            tx,
            rx,
            pending: HashMap::new(),
            next_slot: 0,
        }
    }

    pub(crate) fn can_schedule(&self) -> bool {
        self.pending.len() < S3_PIPELINE_CAPACITY
    }

    pub(crate) fn schedule_inner<T: bytemuck::Pod>(
        &mut self,
        user_data: U,
        store: Arc<dyn ObjectStore>,
        key: object_store::path::Path,
        range: ReadRange,
    ) -> Result<()> {
        if !self.can_schedule() {
            return Err(UniversalIoError::QueueIsFull);
        }
        let slot = self.next_slot;
        self.next_slot = self.next_slot.wrapping_add(1);
        self.pending.insert(slot, user_data);

        let item_size = size_of::<T>() as u64;
        let start = range.byte_offset;
        let end = start + range.length * item_size;

        let req = S3Request {
            store,
            key,
            range: start..end,
            tx: self.tx.clone(),
            slot,
        };
        self.runtime
            .tx()
            .try_send(req)
            .map_err(|_| UniversalIoError::S3RuntimeShutDown)
    }

    pub(crate) fn wait_inner<T: bytemuck::Pod>(&mut self) -> Result<Option<(U, Vec<T>)>> {
        if self.pending.is_empty() {
            return Ok(None);
        }
        let response = self
            .rx
            .blocking_recv()
            .expect("tx held by self; cannot disconnect");
        let user_data = self
            .pending
            .remove(&response.slot)
            .expect("response slot must be in pending");
        let bytes = response.bytes?;
        let items = bytemuck::try_cast_slice(&bytes)?;
        Ok(Some((user_data, items.to_vec())))
    }
}

pub struct BorrowedS3ReadPipeline<'file, T, U> {
    inner: Option<S3PipelineInner<U>>,
    _phantom: PhantomData<(&'file (), T)>,
}

impl<'file, T, U> BorrowedReadPipeline<'file, T, U> for BorrowedS3ReadPipeline<'file, T, U>
where
    T: bytemuck::Pod,
    U: UserData,
{
    type File = S3File;

    fn new() -> Result<Self> {
        Ok(Self {
            inner: None,
            _phantom: PhantomData,
        })
    }

    fn can_schedule(&mut self) -> bool {
        self.inner.as_ref().is_none_or(|i| i.can_schedule())
    }

    fn schedule<P: AccessPattern>(
        &mut self,
        user_data: U,
        file: &'file S3File,
        range: ReadRange,
    ) -> Result<()> {
        let inner = self
            .inner
            .get_or_insert_with(|| S3PipelineInner::new(file.runtime.clone()));
        inner.schedule_inner::<T>(user_data, file.store.clone(), file.key.clone(), range)
    }

    fn wait(&mut self) -> Result<Option<(U, Cow<'file, [T]>)>> {
        let Some(inner) = self.inner.as_mut() else {
            return Ok(None);
        };
        Ok(inner.wait_inner::<T>()?.map(|(u, v)| (u, Cow::Owned(v))))
    }
}

pub struct OwnedS3ReadPipeline<T, U> {
    file: S3File,
    inner: S3PipelineInner<U>,
    _phantom: PhantomData<T>,
}

impl<T, U> OwnedReadPipeline<T, U> for OwnedS3ReadPipeline<T, U>
where
    T: bytemuck::Pod,
    U: UserData,
{
    type File = S3File;

    fn new(file: S3File) -> Result<Self> {
        let runtime = file.runtime.clone();
        Ok(Self {
            file,
            inner: S3PipelineInner::new(runtime),
            _phantom: PhantomData,
        })
    }

    fn can_schedule(&mut self) -> bool {
        self.inner.can_schedule()
    }

    fn schedule<P: AccessPattern>(&mut self, user_data: U, range: ReadRange) -> Result<()> {
        self.inner.schedule_inner::<T>(
            user_data,
            self.file.store.clone(),
            self.file.key.clone(),
            range,
        )
    }

    fn wait(&mut self) -> Result<Option<(U, Cow<'_, [T]>)>> {
        Ok(self
            .inner
            .wait_inner::<T>()?
            .map(|(u, v)| (u, Cow::Owned(v))))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::{ObjectStore, ObjectStoreExt as _};

    use super::*;

    fn fresh_store_with(
        objects: &[(&str, &'static [u8])],
    ) -> (S3RuntimeHandle, Arc<dyn ObjectStore>) {
        let runtime = S3RuntimeHandle::global();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        runtime.block_on(async {
            for (k, v) in objects {
                store
                    .put(
                        &object_store::path::Path::from(*k),
                        Bytes::from_static(v).into(),
                    )
                    .await
                    .unwrap();
            }
        });
        (runtime, store)
    }

    #[test]
    fn can_schedule_starts_true_and_blocks_when_full() {
        let (runtime, _store) = fresh_store_with(&[]);
        let mut inner: S3PipelineInner<u32> = S3PipelineInner::new(runtime);
        assert!(inner.can_schedule());

        for i in 0..S3_PIPELINE_CAPACITY as u64 {
            inner.pending.insert(i, i as u32);
        }
        assert!(!inner.can_schedule());
    }

    #[test]
    fn schedule_and_wait_round_trip() {
        let (runtime, store) = fresh_store_with(&[("k0", b"hello")]);
        let mut inner: S3PipelineInner<u32> = S3PipelineInner::new(runtime);

        inner
            .schedule_inner::<u8>(
                111,
                store.clone(),
                object_store::path::Path::from("k0"),
                ReadRange::new(0, 5),
            )
            .expect("schedule");

        let (user, bytes) = inner.wait_inner::<u8>().expect("wait ok").expect("some");
        assert_eq!(user, 111);
        assert_eq!(&bytes[..], b"hello");
    }

    #[test]
    fn out_of_order_completion_preserves_user_data() {
        let (runtime, store) =
            fresh_store_with(&[("k0", b"aaaa"), ("k1", b"bb"), ("k2", b"cccccc")]);
        let mut inner: S3PipelineInner<u32> = S3PipelineInner::new(runtime);

        for (i, (k, data)) in [
            ("k0", b"aaaa".as_slice()),
            ("k1", b"bb".as_slice()),
            ("k2", b"cccccc".as_slice()),
        ]
        .iter()
        .enumerate()
        {
            inner
                .schedule_inner::<u8>(
                    i as u32,
                    store.clone(),
                    object_store::path::Path::from(*k),
                    ReadRange::new(0, data.len() as u64),
                )
                .unwrap();
        }

        let mut seen = std::collections::HashSet::new();
        for _ in 0..3 {
            let (user, _bytes) = inner.wait_inner::<u8>().unwrap().unwrap();
            assert!(seen.insert(user), "user_data {user} seen twice");
        }
        assert_eq!(seen, [0u32, 1, 2].into_iter().collect());
        assert!(inner.wait_inner::<u8>().unwrap().is_none());
    }

    #[test]
    fn schedule_returns_queue_full_when_capacity_reached() {
        let (runtime, _) = fresh_store_with(&[]);
        let mut inner: S3PipelineInner<u32> = S3PipelineInner::new(runtime);
        for i in 0..S3_PIPELINE_CAPACITY as u64 {
            inner.pending.insert(i, i as u32);
        }
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let err = inner
            .schedule_inner::<u8>(
                999,
                store,
                object_store::path::Path::from("x"),
                ReadRange::new(0, 1),
            )
            .unwrap_err();
        assert!(matches!(err, UniversalIoError::QueueIsFull));
    }

    #[test]
    fn drop_mid_flight_does_not_panic() {
        let (runtime, store) = fresh_store_with(&[("k", b"data!")]);
        {
            let mut inner: S3PipelineInner<u32> = S3PipelineInner::new(runtime);
            for i in 0..10 {
                inner
                    .schedule_inner::<u8>(
                        i,
                        store.clone(),
                        object_store::path::Path::from("k"),
                        ReadRange::new(0, 5),
                    )
                    .unwrap();
            }
            // Drop without calling wait_inner.
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}
