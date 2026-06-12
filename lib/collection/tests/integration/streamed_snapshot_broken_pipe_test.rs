//! Deterministic reproduction of the production error:
//!
//! ```text
//! ERROR collection::shards::shard_holder: Failed to stream shard snapshot:
//!   Service internal error: Applying function to a proxied shard segment 5 failed:
//!   Service runtime error: failed to add quantized vectors file
//!   ./storage/collections/<c>/3/segments/<uuid>/vector_storage/quantized.data
//!   into snapshot: broken pipe
//! ```
//!
//! Mechanism (see [`ShardHolder::stream_shard_snapshot`]): a shard snapshot is
//! tarred on a blocking thread into the *write* half of a `tokio::io::duplex`,
//! while the *read* half is the HTTP response body that the receiving peer
//! drains during a snapshot-method shard transfer. If that receiver's
//! connection drops mid-stream, the read half is dropped and the next write to
//! the duplex returns [`std::io::ErrorKind::BrokenPipe`]. That error surfaces
//! from `Segment::snapshot_files` via `failed_to_add(..)` as
//! `failed to add <file> into snapshot: broken pipe`.
//!
//! In the incident `kill-random-node` deleted the receiving pod mid-transfer;
//! the file in flight happened to be `quantized.data`, but the very same loop
//! (`lib/segment/src/segment/snapshot.rs`, the `blocking_append_file` calls for
//! vector index / vector storage / quantized / payload / id-tracker files)
//! produces the identical error for whichever segment file is being streamed
//! when the consumer goes away.
//!
//! This test recreates that exact pipeline — `duplex(4096)` + `SyncIoBridge` +
//! `BuilderExt::new_streaming_owned` + `Segment::take_snapshot(Streamable)` —
//! and drops the consumer to get a deterministic broken pipe. No timing, no
//! kernel RST, no network: dropping the duplex read half is a hard contract.
//!
//! [`ShardHolder::stream_shard_snapshot`]: collection::shards::shard_holder

use std::collections::HashMap;

use common::counter::hardware_counter::HardwareCounterCell;
use common::tar_ext::BuilderExt;
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, only_default_vector};
use segment::entry::entry_point::SegmentEntry as _;
use segment::entry::snapshot_entry::SnapshotEntry as _;
use segment::segment_constructor::segment_builder::SegmentBuilder;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{
    Distance, HnswConfig, HnswGlobalConfig, Indexes, PayloadStorageType, SegmentConfig,
    SnapshotFormat, VectorDataConfig, VectorStorageType,
};
use tempfile::Builder;
use tokio_util::io::SyncIoBridge;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streamed_shard_snapshot_consumer_drop_yields_broken_pipe() {
    let _ = env_logger::builder().is_test(true).try_init();

    // --- Build an on-disk segment with real files to stream (like a shard) ---
    let source_dir = Builder::new().prefix("segment_src").tempdir().unwrap();
    let mut source = build_simple_segment(source_dir.path(), 4, Distance::Dot).unwrap();

    let hw = HardwareCounterCell::new();
    for i in 0..512u64 {
        source
            .upsert_point(
                i,
                i.into(),
                only_default_vector(&[i as f32, 1.0, 2.0, 3.0]),
                &hw,
            )
            .unwrap();
    }

    // Finalize into an mmap/on-disk segment so `take_snapshot` has actual files
    // to append (mirrors `segment_on_disk_snapshot.rs`).
    let segment_config = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: 4,
                distance: Distance::Dot,
                storage_type: VectorStorageType::Mmap,
                index: Indexes::Hnsw(HnswConfig {
                    m: 4,
                    ef_construct: 16,
                    full_scan_threshold: 8,
                    max_indexing_threads: 2,
                    on_disk: Some(true),
                    payload_m: None,
                    inline_storage: None,
                }),
                quantization_config: None,
                multivector_config: None,
                datatype: None,
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: PayloadStorageType::Mmap,
    };

    let build_dir = Builder::new().prefix("segment_build").tempdir().unwrap();
    let base_dir = Builder::new().prefix("segment_base").tempdir().unwrap();
    let mut builder = SegmentBuilder::new(
        build_dir.path(),
        &segment_config,
        &HnswGlobalConfig::default(),
    )
    .unwrap();
    builder.update(&[&source], &false.into(), &hw).unwrap();
    let segment = builder.build_for_test(base_dir.path());

    // --- Mirror `stream_shard_snapshot`: tar into the write half of a duplex ---
    let (read_half, write_half) = tokio::io::duplex(4096);

    // The receiver "dies": drop the read half so the very next write to the
    // duplex returns BrokenPipe (tokio duplex contract; see the note in
    // `lib/collection/src/common/timeout_writer.rs` tests).
    drop(read_half);

    let tar = BuilderExt::new_streaming_owned(SyncIoBridge::new(write_half));

    let temp_dir = Builder::new().prefix("snapshot_temp").tempdir().unwrap();
    let temp_path = temp_dir.path().to_path_buf();

    // `take_snapshot` performs blocking tar writes (and `SyncIoBridge` blocks on
    // the duplex), so it must run off the runtime workers — exactly like the
    // real code, which drives the snapshot on a blocking thread.
    let result = tokio::task::spawn_blocking(move || {
        segment.take_snapshot(&temp_path, &tar, SnapshotFormat::Streamable, None)
    })
    .await
    .expect("snapshot task panicked");

    let err = result.expect_err("snapshot must fail once the stream consumer is gone");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("into snapshot") && msg.contains("broken pipe"),
        "expected a 'failed to add <file> into snapshot: broken pipe' error, got: {err}",
    );
}
