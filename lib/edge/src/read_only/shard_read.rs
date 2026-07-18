use std::path::Path;
use std::sync::Arc;

use parking_lot::RwLock;
use segment::index::UniversalReadExt;
use segment::segment::read_only::ReadOnlySegment;

use crate::EdgeConfig;
use crate::read_only::ReadOnlyEdgeShard;
use crate::read_view::ReadViewProvider;

/// The follower's segments are homogeneous, so its handle is the concrete
/// `Arc<RwLock<ReadOnlySegment<S>>>` — the read path is fully monomorphized, no dynamic dispatch.
/// The [`EdgeShardRead`](crate::EdgeShardRead) read API comes through the blanket impl over this.
impl<S: UniversalReadExt + 'static> ReadViewProvider for ReadOnlyEdgeShard<S>
where
    S::Fs: Send + Sync,
{
    type Handle = Arc<RwLock<ReadOnlySegment<S>>>;

    fn read_segments(&self) -> Vec<Arc<RwLock<ReadOnlySegment<S>>>> {
        self.segments.read().read_handles()
    }

    fn config_snapshot(&self) -> Arc<EdgeConfig> {
        self.config.read().clone()
    }

    fn search_pool(&self) -> Arc<rayon::ThreadPool> {
        self.search_pool.clone()
    }

    fn path(&self) -> &Path {
        &self.path
    }
}
