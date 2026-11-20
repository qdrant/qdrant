//! Payload blocks of a remote build, streamed while core prepares them.
//!
//! Core appends blocks as it reads them from the payload index, and finishes the stream when
//! there are no more. The indexer reads them by sequence number, after building the main
//! graph, so core's payload reads overlap with the indexer's main graph build. The stream is
//! shared by all attempts of a build: a retry serves the blocks prepared already, and a local
//! fallback builds from them.

use std::time::{Duration, Instant};

use common::types::PointOffsetType;
use parking_lot::{Condvar, Mutex};

use super::wire::{BlockPlan, PlannedBlock};

/// Max size of the points of one batch of blocks. A larger block is sent alone.
pub const BLOCKS_BATCH_BYTES: usize = 1 << 20;

/// One payload block, with the field it belongs to.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamedBlock {
    /// Name of the field, for progress reporting.
    pub field: String,
    /// Skip the block if the graph built so far already connects it well. False for the first
    /// field and for tenant fields, whose blocks are always built.
    pub skip_if_connected: bool,
    /// The payload index's estimate of the block size. The size limit is checked against this.
    pub cardinality: usize,
    /// Non-deleted point offsets of the block, in the segment under construction.
    pub points: Vec<PointOffsetType>,
}

/// Answer to a read of the blocks from a sequence number.
#[derive(Debug, Clone, PartialEq)]
pub enum BlocksBatch {
    /// Blocks from the requested sequence number, in order.
    Blocks {
        first: u64,
        blocks: Vec<StreamedBlock>,
    },
    /// No block at the requested sequence number yet: ask again.
    NotReady,
    /// No block at or after the requested sequence number, and none will come.
    Finished,
}

#[derive(Default)]
struct StreamState {
    plan: BlockPlan,
    /// Field and block index in `plan` of each block, by sequence number.
    order: Vec<(usize, usize)>,
    finished: bool,
}

/// Append-only stream of payload blocks, in build order.
#[derive(Default)]
pub struct BlockStream {
    state: Mutex<StreamState>,
    changed: Condvar,
}

impl BlockStream {
    /// Append a block of `field`. Blocks of a field must be appended together, fields in build
    /// order.
    pub fn push(&self, field: &str, skip_if_connected: bool, block: PlannedBlock) {
        let mut state = self.state.lock();
        debug_assert!(!state.finished, "block pushed after the stream finished");
        let StreamState { plan, order, .. } = &mut *state;
        let field_index = plan.push(field, skip_if_connected, block);
        let block_index = plan.fields[field_index].blocks.len() - 1;
        order.push((field_index, block_index));
        drop(state);
        self.changed.notify_all();
    }

    /// No more blocks will be appended.
    pub fn finish(&self) {
        self.state.lock().finished = true;
        self.changed.notify_all();
    }

    pub fn is_finished(&self) -> bool {
        self.state.lock().finished
    }

    /// The blocks from `next`, up to about [`BLOCKS_BATCH_BYTES`]. Waits up to `wait` for a
    /// block to be appended, or for the stream to finish, before answering
    /// [`BlocksBatch::NotReady`].
    pub fn read(&self, next: u64, wait: Duration) -> BlocksBatch {
        let deadline = Instant::now() + wait;
        let mut state = self.state.lock();
        loop {
            let available = state.order.len() as u64;
            if next < available {
                return Self::batch(&state, next);
            }
            if state.finished {
                return BlocksBatch::Finished;
            }
            if self.changed.wait_until(&mut state, deadline).timed_out() {
                return BlocksBatch::NotReady;
            }
        }
    }

    fn batch(state: &StreamState, first: u64) -> BlocksBatch {
        let mut blocks = Vec::new();
        let mut size = 0;
        for &(field_index, block_index) in &state.order[first as usize..] {
            let field = &state.plan.fields[field_index];
            let block = &field.blocks[block_index];
            let block_size = block.points.len() * size_of::<PointOffsetType>();
            if !blocks.is_empty() && size + block_size > BLOCKS_BATCH_BYTES {
                break;
            }
            size += block_size;
            blocks.push(StreamedBlock {
                field: field.field.clone(),
                skip_if_connected: field.skip_if_connected,
                cardinality: block.cardinality,
                points: block.points.clone(),
            });
        }
        BlocksBatch::Blocks { first, blocks }
    }

    /// All blocks, once the stream is finished.
    pub fn finished_plan(&self) -> Option<BlockPlan> {
        let state = self.state.lock();
        state.finished.then(|| state.plan.clone())
    }

    /// Number of points over all blocks, once the stream is finished.
    pub fn finished_num_points(&self) -> Option<usize> {
        let state = self.state.lock();
        state.finished.then(|| state.plan.num_points())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn block(points: &[PointOffsetType]) -> PlannedBlock {
        PlannedBlock {
            cardinality: points.len(),
            points: points.to_vec(),
        }
    }

    #[test]
    fn reads_by_sequence_number() {
        let stream = BlockStream::default();
        assert_eq!(stream.read(0, Duration::ZERO), BlocksBatch::NotReady);

        stream.push("a", false, block(&[1, 2]));
        stream.push("a", false, block(&[3]));
        stream.push("b", true, block(&[4]));
        let BlocksBatch::Blocks { first, blocks } = stream.read(1, Duration::ZERO) else {
            panic!("expected blocks");
        };
        assert_eq!(first, 1);
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].points, vec![3]);
        assert_eq!(
            (blocks[1].field.as_str(), blocks[1].skip_if_connected),
            ("b", true)
        );
        // Reading the same sequence number again gives the same blocks
        assert_eq!(
            stream.read(1, Duration::ZERO),
            BlocksBatch::Blocks { first, blocks }
        );

        assert_eq!(stream.read(3, Duration::ZERO), BlocksBatch::NotReady);
        assert!(stream.finished_plan().is_none());
        stream.finish();
        assert_eq!(stream.read(3, Duration::ZERO), BlocksBatch::Finished);
        let plan = stream.finished_plan().unwrap();
        assert_eq!(plan.fields.len(), 2);
        assert_eq!(plan.num_points(), 4);
    }

    #[test]
    fn read_waits_for_the_next_block() {
        let stream = Arc::new(BlockStream::default());
        let pusher = {
            let stream = stream.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(50));
                stream.push("a", false, block(&[7]));
            })
        };
        let batch = stream.read(0, Duration::from_secs(10));
        assert!(matches!(batch, BlocksBatch::Blocks { first: 0, .. }));
        pusher.join().unwrap();
    }

    #[test]
    fn batches_are_bounded() {
        let stream = BlockStream::default();
        let large: Vec<PointOffsetType> = (0..(BLOCKS_BATCH_BYTES / 4) as u32).collect();
        stream.push("a", false, block(&large));
        stream.push("a", false, block(&large));
        let BlocksBatch::Blocks { blocks, .. } = stream.read(0, Duration::ZERO) else {
            panic!("expected blocks");
        };
        assert_eq!(blocks.len(), 1);
    }
}
