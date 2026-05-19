use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use common::universal_io::UniversalIoError;
use tokio::runtime::Runtime;

use crate::{AsyncDispatcher, AsyncReadBackend, IoBridgeFile};

pub(super) struct StaticMockBackend {
    contents: Vec<u8>,
    delay: Option<Duration>,
    delays_by_offset: HashMap<u64, Duration>,
}

impl StaticMockBackend {
    pub(super) fn new(contents: Vec<u8>) -> Self {
        Self {
            contents,
            delay: None,
            delays_by_offset: HashMap::new(),
        }
    }

    pub(super) fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    pub(super) fn with_delays_by_offset(mut self, delays: HashMap<u64, Duration>) -> Self {
        self.delays_by_offset = delays;
        self
    }
}

impl AsyncReadBackend for StaticMockBackend {
    type Location = ();

    fn read_bytes(
        &self,
        _location: Self::Location,
        byte_offset: u64,
        byte_length: u64,
    ) -> impl Future<Output = Result<Vec<u8>, UniversalIoError>> + Send {
        let start = byte_offset as usize;
        let end = start + byte_length as usize;
        let slice = self.contents[start..end].to_vec();
        let delay = self
            .delays_by_offset
            .get(&byte_offset)
            .copied()
            .or(self.delay);
        async move {
            if let Some(d) = delay {
                tokio::time::sleep(d).await;
            }
            Ok(slice)
        }
    }
}

pub(super) struct FailingMockBackend;

impl AsyncReadBackend for FailingMockBackend {
    type Location = ();

    fn read_bytes(
        &self,
        _location: Self::Location,
        _byte_offset: u64,
        _byte_length: u64,
    ) -> impl Future<Output = Result<Vec<u8>, UniversalIoError>> + Send {
        async { Err(UniversalIoError::uninitialized("simulated backend failure")) }
    }
}

pub(super) struct FailingMockFile {
    pub(super) dispatcher: Arc<AsyncDispatcher<FailingMockBackend>>,
}

impl IoBridgeFile for FailingMockFile {
    type Backend = FailingMockBackend;

    fn dispatcher(&self) -> &Arc<AsyncDispatcher<Self::Backend>> {
        &self.dispatcher
    }

    fn location(&self) {}
}

pub(super) struct MockFile {
    pub(super) dispatcher: Arc<AsyncDispatcher<StaticMockBackend>>,
}

impl IoBridgeFile for MockFile {
    type Backend = StaticMockBackend;

    fn dispatcher(&self) -> &Arc<AsyncDispatcher<Self::Backend>> {
        &self.dispatcher
    }

    fn location(&self) {}
}

pub(super) fn build_pipeline_with_static_contents(contents: &[u8]) -> (Runtime, MockFile) {
    build_pipeline_with_backend(StaticMockBackend::new(contents.to_vec()))
}

pub(super) fn build_pipeline_with_backend(backend: StaticMockBackend) -> (Runtime, MockFile) {
    let runtime = Runtime::new().expect("runtime");
    let handle = runtime.handle().clone();
    let dispatcher = Arc::new(AsyncDispatcher::new(handle, backend));
    let file = MockFile { dispatcher };
    (runtime, file)
}
