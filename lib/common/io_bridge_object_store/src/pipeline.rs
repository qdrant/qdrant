use std::borrow::Cow;
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;

use bytes::Bytes;
use common::generic_consts::AccessPattern;
use common::universal_io::{
    BorrowedReadPipeline, OwnedReadPipeline, ReadRange, Result, UniversalIoError, UserData,
};
use tokio::sync::mpsc;

use crate::file::BlobFile;
use crate::read::BlobRead;
use crate::runtime::{BridgeRequest, BridgeResponse, BridgeRuntime};

pub(crate) const BLOB_PIPELINE_CAPACITY: usize = 256;

/// Pipeline-side bookkeeping. Owns only the *reply* channel and an optional
/// default runtime. Every `schedule` call resolves which runtime to dispatch
/// onto using: caller-supplied override → pipeline default → global.
pub(crate) struct PipelineInner<U> {
    default_runtime: Option<BridgeRuntime>,
    tx: mpsc::Sender<BridgeResponse>,
    rx: mpsc::Receiver<BridgeResponse>,
    pub(crate) pending: HashMap<u64, U>,
    next_slot: u64,
}

impl<U> PipelineInner<U>
where
    U: UserData,
{
    pub(crate) fn new(default_runtime: Option<BridgeRuntime>) -> Self {
        let (tx, rx) = mpsc::channel(BLOB_PIPELINE_CAPACITY);
        Self {
            default_runtime,
            tx,
            rx,
            pending: HashMap::new(),
            next_slot: 0,
        }
    }

    pub(crate) fn can_schedule(&self) -> bool {
        self.pending.len() < BLOB_PIPELINE_CAPACITY
    }

    pub(crate) fn schedule(
        &mut self,
        runtime: Option<&BridgeRuntime>,
        user_data: U,
        future: Pin<Box<dyn Future<Output = Result<Bytes>> + Send + 'static>>,
    ) -> Result<()> {
        if !self.can_schedule() {
            return Err(UniversalIoError::QueueIsFull);
        }
        let slot = self.next_slot;
        self.next_slot = self.next_slot.wrapping_add(1);
        self.pending.insert(slot, user_data);

        let req = BridgeRequest {
            future,
            tx: self.tx.clone(),
            slot,
        };
        let tx = if let Some(rt) = runtime {
            rt.tx()
        } else if let Some(rt) = &self.default_runtime {
            rt.tx()
        } else {
            BridgeRuntime::global().tx()
        };
        tx.try_send(req)
            .map_err(|_| UniversalIoError::S3RuntimeShutDown)
    }

    pub(crate) fn wait<T: bytemuck::Pod>(&mut self) -> Result<Option<(U, Vec<T>)>> {
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

pub struct BorrowedBlobPipeline<'file, A: BlobRead, T, U> {
    inner: Option<PipelineInner<U>>,
    _phantom: PhantomData<(&'file BlobFile<A>, T)>,
}

impl<'file, A, T, U> BorrowedReadPipeline<'file, T, U> for BorrowedBlobPipeline<'file, A, T, U>
where
    A: BlobRead,
    T: bytemuck::Pod,
    U: UserData,
{
    type File = BlobFile<A>;

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
        file: &'file BlobFile<A>,
        range: ReadRange,
    ) -> Result<()> {
        let inner = self.inner.get_or_insert_with(|| PipelineInner::new(None));
        let item_size = size_of::<T>() as u64;
        let start = range.byte_offset;
        let end = start + range.length * item_size;
        let future = file.inner.read_range(start..end);
        inner.schedule(Some(&file.runtime), user_data, future)
    }

    fn wait(&mut self) -> Result<Option<(U, Cow<'file, [T]>)>> {
        let Some(inner) = self.inner.as_mut() else {
            return Ok(None);
        };
        Ok(inner.wait::<T>()?.map(|(u, v)| (u, Cow::Owned(v))))
    }
}

pub struct OwnedBlobPipeline<A: BlobRead, T, U> {
    file: BlobFile<A>,
    inner: PipelineInner<U>,
    _phantom: PhantomData<T>,
}

impl<A, T, U> OwnedReadPipeline<T, U> for OwnedBlobPipeline<A, T, U>
where
    A: BlobRead,
    T: bytemuck::Pod,
    U: UserData,
{
    type File = BlobFile<A>;

    fn new(file: BlobFile<A>) -> Result<Self> {
        let default_runtime = Some(file.runtime.clone());
        Ok(Self {
            file,
            inner: PipelineInner::new(default_runtime),
            _phantom: PhantomData,
        })
    }

    fn can_schedule(&mut self) -> bool {
        self.inner.can_schedule()
    }

    fn schedule<P: AccessPattern>(&mut self, user_data: U, range: ReadRange) -> Result<()> {
        let item_size = size_of::<T>() as u64;
        let start = range.byte_offset;
        let end = start + range.length * item_size;
        let future = self.file.inner.read_range(start..end);
        self.inner.schedule(None, user_data, future)
    }

    fn wait(&mut self) -> Result<Option<(U, Cow<'_, [T]>)>> {
        Ok(self.inner.wait::<T>()?.map(|(u, v)| (u, Cow::Owned(v))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipeline_can_schedule_starts_true_and_blocks_when_full() {
        let mut inner: PipelineInner<u32> = PipelineInner::new(None);
        assert!(inner.can_schedule());
        for i in 0..BLOB_PIPELINE_CAPACITY as u64 {
            inner.pending.insert(i, i as u32);
        }
        assert!(!inner.can_schedule());
    }

    #[test]
    fn pipeline_schedule_returns_queue_full_when_capacity_reached() {
        let runtime = BridgeRuntime::global();
        let mut inner: PipelineInner<u32> = PipelineInner::new(None);
        for i in 0..BLOB_PIPELINE_CAPACITY as u64 {
            inner.pending.insert(i, i as u32);
        }
        let err = inner
            .schedule(
                Some(&runtime),
                999,
                Box::pin(async { Ok(Bytes::from_static(b"x")) }),
            )
            .unwrap_err();
        assert!(matches!(err, UniversalIoError::QueueIsFull));
    }

    #[test]
    fn pipeline_schedule_and_wait_round_trip() {
        let runtime = BridgeRuntime::global();
        let mut inner: PipelineInner<u32> = PipelineInner::new(None);
        inner
            .schedule(
                Some(&runtime),
                111,
                Box::pin(async { Ok(Bytes::from_static(b"hello")) }),
            )
            .expect("schedule");
        let (user, bytes) = inner.wait::<u8>().expect("wait ok").expect("some");
        assert_eq!(user, 111);
        assert_eq!(&bytes[..], b"hello");
    }

    #[test]
    fn pipeline_out_of_order_completion_preserves_user_data() {
        let runtime = BridgeRuntime::global();
        let mut inner: PipelineInner<u32> = PipelineInner::new(None);
        for (i, bytes) in [b"aaaa".as_slice(), b"bb".as_slice(), b"cccccc".as_slice()]
            .iter()
            .enumerate()
        {
            let bytes = *bytes;
            inner
                .schedule(
                    Some(&runtime),
                    i as u32,
                    Box::pin(async move { Ok(Bytes::from_static(bytes)) }),
                )
                .unwrap();
        }
        let mut seen = std::collections::HashSet::new();
        for _ in 0..3 {
            let (user, _bytes) = inner.wait::<u8>().unwrap().unwrap();
            assert!(seen.insert(user), "user_data {user} seen twice");
        }
        assert_eq!(seen, [0u32, 1, 2].into_iter().collect());
        assert!(inner.wait::<u8>().unwrap().is_none());
    }

    /// Two distinct runtimes feed reply responses into one pipeline; both
    /// runtimes execute work in parallel and the pipeline accepts both replies.
    #[test]
    fn pipeline_collects_replies_from_multiple_runtimes() {
        let rt_a = BridgeRuntime::new().expect("rt_a");
        let rt_b = BridgeRuntime::new().expect("rt_b");
        let mut inner: PipelineInner<u32> = PipelineInner::new(None);
        inner
            .schedule(
                Some(&rt_a),
                1,
                Box::pin(async { Ok(Bytes::from_static(b"AAAA")) }),
            )
            .unwrap();
        inner
            .schedule(
                Some(&rt_b),
                2,
                Box::pin(async { Ok(Bytes::from_static(b"BB")) }),
            )
            .unwrap();
        let mut seen: std::collections::HashMap<u32, Vec<u8>> = std::collections::HashMap::new();
        for _ in 0..2 {
            let (user, bytes) = inner.wait::<u8>().unwrap().unwrap();
            seen.insert(user, bytes);
        }
        assert_eq!(seen[&1], b"AAAA");
        assert_eq!(seen[&2], b"BB");
    }

    /// When schedule's override is None, the pipeline default runtime is used.
    #[test]
    fn pipeline_uses_default_runtime_when_override_is_none() {
        let rt_default = BridgeRuntime::new().expect("default rt");
        let mut inner: PipelineInner<u32> = PipelineInner::new(Some(rt_default));
        inner
            .schedule(None, 42, Box::pin(async { Ok(Bytes::from_static(b"def")) }))
            .expect("schedule");
        let (user, bytes) = inner.wait::<u8>().expect("wait ok").expect("some");
        assert_eq!(user, 42);
        assert_eq!(&bytes[..], b"def");
    }

    /// When both schedule override and pipeline default are None, the global
    /// runtime is used.
    #[test]
    fn pipeline_falls_back_to_global_when_both_none() {
        let mut inner: PipelineInner<u32> = PipelineInner::new(None);
        inner
            .schedule(None, 7, Box::pin(async { Ok(Bytes::from_static(b"glb")) }))
            .expect("schedule");
        let (user, bytes) = inner.wait::<u8>().expect("wait ok").expect("some");
        assert_eq!(user, 7);
        assert_eq!(&bytes[..], b"glb");
    }
}
