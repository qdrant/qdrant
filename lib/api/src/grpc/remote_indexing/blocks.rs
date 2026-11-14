//! Conversions of the payload blocks streamed with `ReadPayloadBlocks`.

use segment::index::hnsw_index::remote::blocks::{BlocksBatch, StreamedBlock};

use crate::grpc::qdrant::{
    IndexingPayloadBlock, IndexingPayloadBlocks, IndexingPayloadBlocksBatch,
    IndexingPayloadBlocksFinished, IndexingPayloadBlocksNotReady, indexing_payload_blocks_batch,
};

pub fn blocks_batch_to_grpc(batch: BlocksBatch) -> IndexingPayloadBlocksBatch {
    let result = match batch {
        BlocksBatch::Blocks { first, blocks } => {
            indexing_payload_blocks_batch::Result::Blocks(IndexingPayloadBlocks {
                first,
                blocks: blocks.into_iter().map(block_to_grpc).collect(),
            })
        }
        BlocksBatch::NotReady => {
            indexing_payload_blocks_batch::Result::NotReady(IndexingPayloadBlocksNotReady {})
        }
        BlocksBatch::Finished => {
            indexing_payload_blocks_batch::Result::Finished(IndexingPayloadBlocksFinished {})
        }
    };
    IndexingPayloadBlocksBatch {
        result: Some(result),
    }
}

pub fn blocks_batch_from_grpc(batch: IndexingPayloadBlocksBatch) -> Result<BlocksBatch, String> {
    let IndexingPayloadBlocksBatch { result } = batch;
    match result {
        Some(indexing_payload_blocks_batch::Result::Blocks(IndexingPayloadBlocks {
            first,
            blocks,
        })) => Ok(BlocksBatch::Blocks {
            first,
            blocks: blocks.into_iter().map(block_from_grpc).collect(),
        }),
        Some(indexing_payload_blocks_batch::Result::NotReady(IndexingPayloadBlocksNotReady {})) => {
            Ok(BlocksBatch::NotReady)
        }
        Some(indexing_payload_blocks_batch::Result::Finished(IndexingPayloadBlocksFinished {})) => {
            Ok(BlocksBatch::Finished)
        }
        None => Err("empty payload blocks batch".to_string()),
    }
}

fn block_to_grpc(block: StreamedBlock) -> IndexingPayloadBlock {
    let StreamedBlock {
        field,
        skip_if_connected,
        cardinality,
        points,
    } = block;
    IndexingPayloadBlock {
        field,
        skip_if_connected,
        cardinality: cardinality as u64,
        points,
    }
}

fn block_from_grpc(block: IndexingPayloadBlock) -> StreamedBlock {
    let IndexingPayloadBlock {
        field,
        skip_if_connected,
        cardinality,
        points,
    } = block;
    StreamedBlock {
        field,
        skip_if_connected,
        cardinality: cardinality as usize,
        points,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batches_roundtrip() {
        let batches = [
            BlocksBatch::Blocks {
                first: 7,
                blocks: vec![StreamedBlock {
                    field: "city".to_string(),
                    skip_if_connected: true,
                    cardinality: 3,
                    points: vec![1, 5, 9],
                }],
            },
            BlocksBatch::NotReady,
            BlocksBatch::Finished,
        ];
        for batch in batches {
            assert_eq!(
                blocks_batch_from_grpc(blocks_batch_to_grpc(batch.clone())).unwrap(),
                batch
            );
        }
        assert!(blocks_batch_from_grpc(IndexingPayloadBlocksBatch { result: None }).is_err());
    }
}
