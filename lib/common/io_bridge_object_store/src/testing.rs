//! In-memory backend for tests that use the object-store IO bridge.

use common::universal_io::{UioResult, UniversalKind};
use object_store::memory::InMemory;

use crate::BlobBackend;

/// No-op configuration for the in-memory backend.
#[derive(Clone, Debug, Default)]
pub struct InMemoryConfig;

impl BlobBackend for InMemory {
    type Config = InMemoryConfig;

    fn build_store(_config: &Self::Config) -> UioResult<Self> {
        Ok(InMemory::new())
    }

    fn kind() -> UniversalKind {
        UniversalKind::S3
    }
}
