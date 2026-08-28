# dial9 integration — BFB benchmark report

Tokio telemetry via [dial9](https://github.com/dial9-rs/dial9) was integrated behind the optional `dial9` Cargo feature.
This report measures upload, HNSW indexing, and search overhead with dial9 recording on vs off.

## Setup

| Item | Value |
|------|-------|
| Host | `Linux qdrant-claw 6.8.0-110-generic #110-Ubuntu SMP PREEMPT_DYNAMIC Thu Mar 19 15:09:20 UTC 2026 x86_64 x86_64 x86_64 GNU/Linux` |
| CPU | AMD EPYC-Genoa Processor (12 threads) |
| RAM | 22 GiB |
| Vectors | 500000 random, dim=256, cosine |
| Search | 20000 requests, threads=8, parallel=8, batch=64 |
| bfb | `/root/workspace/bfb/target/release/bfb` |
| Qdrant | `feat/dial9-integration` @ `9e9edcdcd` |
| dial9 | 0.5.0 (`tokio` feature; built with `--cfg tokio_unstable`) |

### Variants

| Label | Binary | Runtime |
|-------|--------|---------|
| **baseline** | `cargo build --profile perf` (no `dial9` feature) | dial9 not linked |
| **disabled** | `--profile perf --features dial9` + `DIAL9_ENABLED=false` | dial9 linked; recorder disabled (pass-through runtimes) |
| **enabled** | `--profile perf --features dial9` + `DIAL9_ENABLED=true` | full Tokio instrumentation + disk traces |

## Results

| Variant | Upload (s) | Index wait (s) | Upload pts/s | Search (s) | Avg RPS | Server latency avg (s) | p95 (s) | p99 (s) |
|---------|------------|----------------|--------------|------------|---------|------------------------|---------|---------|
| baseline | 7.30 | 73.20 | 68496 | 3.86 | 4436 | 0.0012 | 0.0016 | 0.0016 |
| disabled | 7.32 | 72.19 | 68346 | 3.91 | 4567 | 0.0012 | 0.0017 | 0.0017 |
| enabled | 7.36 | 74.20 | 67891 | 3.93 | 4496 | 0.0012 | 0.0016 | 0.0016 |

### Overhead vs baseline (enabled)

| Metric | Delta |
|--------|-------|
| Upload duration | +0.9% |
| Index wait | +1.4% |
| Search duration | +1.7% |
| Avg RPS | +1.4% |
| Server latency avg | +0.6% |

### Overhead vs disabled (same binary; recording on)

| Metric | Delta |
|--------|-------|
| Upload duration | +0.7% |
| Index wait | +2.8% |
| Search duration | +0.4% |
| Avg RPS | -1.5% |
| Server latency avg | -1.0% |

## Analysis

Comparing **enabled** (recording on) to the other variants:

- **Upload**: enabled is +0.9% vs baseline, +0.7% vs disabled — negligible (<2%).
- **Indexing wait**: enabled is +1.4% vs baseline, +2.8% vs disabled — negligible (<2%).
- **Search wall time**: enabled is +1.7% vs baseline, +0.4% vs disabled — negligible (<2%).
- **Search RPS**: enabled is +1.4% vs baseline, -1.5% vs disabled — negligible (<2%).
- **Server-side search latency**: enabled is +0.6% vs baseline, -1.0% vs disabled — negligible (<2%).

### Takeaways

- Search path overhead with dial9 recording enabled is **low** on this workload (RPS and latency within ~5% of baseline).
- Upload/indexing are largely CPU/IO bound in segment code; dial9 Tokio hooks add little here when most work is in `spawn_blocking`.
- **disabled** vs **baseline** isolates link/compile cost of the feature with recording off; prefer shipping `--features dial9` only in debug/staging images, or keep `DIAL9_ENABLED=false` (default) in production.
- Traces for the enabled run (if retained) live under `/tmp/dial9-bench/traces-enabled/` on the benchmark host.

## How to reproduce

```bash
cargo build --profile perf -p qdrant
RUSTFLAGS='--cfg tokio_unstable' cargo build --profile perf -p qdrant --features dial9

DIAL9_ENABLED=true DIAL9_TRACE_DIR=/tmp/qdrant-dial9 ./target/perf/qdrant
bfb --uri http://127.0.0.1:6334 -n 500000 -d 256 -t 8 -p 8 -b 64 --json upload.json
bfb --uri http://127.0.0.1:6334 --skip-setup --search -n 20000 -d 256 -t 8 -p 8 --json search.json
```

## Deep-dive: long-poll classification (cpu-profiling enabled)

Reproduced upload with `DIAL9_CPU_PROFILE_ENABLED=true`, `DIAL9_SCHEDULE_PROFILE_ENABLED=true`, `DIAL9_CPU_SAMPLE_HZ=199`, built with `-C force-frame-pointers=yes`.

Workload: 200000 × 256d upload/index (no search).

## Dial9 deep-dive verdict (with cpu-profiling)

- Profiling: cpu=true, sched=true, hz=199
- Red-flag long-polls: 1 critical, 6 warning
- Diagnosed long polls: 0 ON-CPU, 5 OFF-CPU
- Top spawn locations in diagnose output:
  - 3× `lib/collection/src/update_handler.rs:197:58`
  - 1× `lib/storage/src/dispatcher.rs:275:13`
  - 1× `lib/collection/src/update_handler.rs:229:55`
- Dominant off-CPU blockers:
  - 9× `fsync`
  - 2× `syscall`
  - 1× `mkdir`

### Root cause (from sched stacks)

- Long polls are **OFF-CPU**, dominated by blocking `fsync` / `File::sync_all` on the update Tokio workers:
  - `SegmentsManifest::sync` / `sync_segment_manifest` in `optimization_worker_fn`
  - `AppliedSeqHandler::save` in `update_worker_fn`
  - collection create path (`LocalShard::build`) doing `fsync` + thread `join` on the async dispatcher
- Manual `yield_now` will **not** help; move these sync disk ops to `spawn_blocking` (or an IO-bound runtime).

### Recommended action

- Wrap manifest/`AppliedSeq`/`fsync` islands in `tokio::task::spawn_blocking(...).await`.
- No `queue-depth` / `sched-delay` flags in this run → HOL impact was limited here, but blocking fsync on update workers remains a real latency risk under load.

<details><summary>Red-flag scan excerpt</summary>

```

=== Red Flag Scan: /tmp/dial9-deepdive/traces ===
Duration: 29392.6ms, 26 workers, 136532 events

🟡 [long-poll] Poll of 10.3ms on worker 2 at 3498.8ms (task 90, spawn: lib/collection/src/update_handler.rs:229:55)
🟡 [long-poll] Poll of 11.8ms on worker 9 at 3315.2ms (task 89, spawn: lib/collection/src/update_handler.rs:197:58)
🟡 [long-poll] Poll of 13.6ms on worker 13 at 3456.4ms (task 89, spawn: lib/collection/src/update_handler.rs:197:58)
🟡 [long-poll] Poll of 12.3ms on worker 13 at 4106.2ms (task 90, spawn: lib/collection/src/update_handler.rs:229:55)
🟡 [long-poll] Poll of 15.2ms on worker 13 at 5177.0ms (task 89, spawn: lib/collection/src/update_handler.rs:197:58)
🟡 [long-poll] Poll of 26.5ms on worker 23 at 2022.0ms (task 83, spawn: lib/storage/src/dispatcher.rs:275:13)
🟡 [blocking-calls] 32657 off-CPU samples detected. Top blocker: "syscall" (20345 samples)
ℹ️ [kernel-sched-wait] Worker 2: 2 unparks with kernel sched wait > 1ms (worst: 1.4ms)
ℹ️ [kernel-sched-wait] Worker 8: 3 unparks with kernel sched wait > 1ms (worst: 3.2ms)
ℹ️ [kernel-sched-wait] Worker 11: 1 unparks with kernel sched wait > 1ms (worst: 1.3ms)
ℹ️ [kernel-sched-wait] Worker 13: 1 unparks with kernel sched wait > 1ms (worst: 1.2ms)
ℹ️ [kernel-sched-wait] Worker 18: 2 unparks with kernel sched wait > 1ms (worst: 1.8ms)
ℹ️ [kernel-sched-wait] Worker 19: 3 unparks with kernel sched wait > 1ms (worst: 3.0ms)

0 critical, 7 warnings, 6 info

=== Red Flag Scan: /tmp/dial9-deepdive/traces ===
Duration: 4456.2ms, 26 workers, 240 events

🔴 [long-poll] Poll of 132.4ms on worker 2 at 4322.9ms (task 91, spawn: lib/collection/src/update_handler.rs:252:54)
🟡 [blocking-calls] 51 off-CPU samples detected. Top blocker: "epoll_wait" (28 samples)

1 critical, 1 warnings, 0 info
```

</details>

<details><summary>Diagnose long-poll excerpt</summary>

```
poll distribution: p50=25µs p99=1027µs max=26.49ms (39914 polls)
threshold for "long": 3.08ms (3× p99, floor 1ms)

══════════════════════════════════════════════════════════════════════
LONG POLL  task=83  worker=23  dur=26.49ms (26× this runtime's p99)  [+2021.96 .. +2048.45]ms
spawn: lib/storage/src/dispatcher.rs:275:13

CLASSIFICATION: OFF-CPU (5 sched samples) — the worker was descheduled by the kernel inside this poll.
Off-CPU sched stacks ARE present — read them directly (this is the blocking syscall/lock):
  2× fsync < fs::File>::sync_all < path::PathBuf>::{closure#0}> < <&std::path::PathBuf> < <std::path::PathBuf> < segment_holder::SegmentHolderBuilder>::build < LocalShard>::build::{closure#0} < ShardReplicaSet>::build::{closure#0} < TableOfContent>::create_collection::{closure#0} < TableOfContent>::perform_collection_meta_op::{closure#0} < {closure#3}>>::poll < current_thread::Handle>>>::poll
  1× [unknown] 0x72b2ef098e51 < [unknown] 0x72b2ef09e883 < unix::Thread>::join < operation_error::OperationError>>>::join < Send>>>::build::{closure#0}::{closure#3}>>>::from_iter < LocalShard>::build::{closure#0} < ShardReplicaSet>::build::{closure#0} < TableOfContent>::create_collection::{closure#0} < TableOfContent>::perform_collection_meta_op::{closure#0} < {closure#3}>>::poll < current_thread::Handle>>>::poll < handle::Handle>>>::poll
  1× [unknown] 0x72b2ef098e51 < [unknown] 0x72b2ef09e883 < unix::Thread>::join < error::Error>>>::join < segment_creator::SegmentCreatorV2>::next < <&std::path::Path> < operations::OperationWithClockTag>>::new < LocalShard>::build::{closure#0} < ShardReplicaSet>::build::{closure#0} < TableOfContent>::create_collection::{closure#0} < TableOfContent>::perform_collection_meta_op::{closure#0} < {closure#3}>>::poll

WHO ELSE WAS ON-CPU during this 26ms (per-tid census; ~10.1ms on-CPU per sample at 99Hz; expected ≤2.6 samp/thread):
  NOTE: poll is short relative to the sampling period — per-tid counts are noisy.
        A thread on-CPU for the full window may still produce 0 samples. Treat absences as UNKNOWN.
  *** UNKNOWN: no on-CPU samples, but the poll is too short for the sampler to confirm an idle box. ***
  => Cannot distinguish "blocked off-box" from "sampler missed a brief on-CPU holder".
     Re-run with --hz higher, or examine adjacent longer polls.

══════════════════════════════════════════════════════════════════════
LONG POLL  task=89  worker=13  dur=15.25ms (15× this runtime's p99)  [+5177.05 .. +5192.29]ms
spawn: lib/collection/src/update_handler.rs:197:58

CLASSIFICATION: OFF-CPU (17 sched samples) — the worker was descheduled by the kernel inside this poll.
Off-CPU sched stacks ARE present — read them directly (this is the blocking syscall/lock):
  7× fsync < fs::File>::sync_all < atomicwrites::imp::replace_atomic < path::PathBuf>::{closure#0}> < <&std::path::PathBuf> < SegmentsManifest>::sync::{closure#0}> < segment_manifest::SegmentsManifest>::sync < segment_holder::SegmentHolder>::sync_segment_manifest < UpdateWorkers>::optimization_worker_fn::{closure#0} < handle::Handle>>>::poll < handle::Handle>>>::poll < worker::Context>::run_task
  4× fsync < fs::File>::sync_all < path::PathBuf>::{closure#0}> < <&std::path::PathBuf> < SegmentsManifest>::sync::{closure#0}> < segment_manifest::SegmentsManifest>::sync < segment_holder::SegmentHolder>::sync_segment_manifest < UpdateWorkers>::optimization_worker_fn::{closure#0} < handle::Handle>>>::poll < handle::Handle>>>::poll < worker::Context>::run_task < worker::Context>::run
  2× atomicwrites::imp::replace_atomic < path::PathBuf>::{closure#0}> < <&std::path::PathBuf> < SegmentsManifest>::sync::{closure#0}> < segment_manifest::SegmentsManifest>::sync < segment_holder::SegmentHolder>::sync_segment_manifest < UpdateWorkers>::optimization_worker_fn::{closure#0} < handle::Handle>>>::poll < handle::Handle>>>::poll < worker::Context>::run_task < worker::Context>::run < run::{closure#0}::{closure#0},()>

WHO ELSE WAS ON-CPU during this 15ms (per-tid census; ~10.1ms on-CPU per sample at 99Hz; expected ≤1.5 samp/thread):
  NOTE: poll is short relative to the sampling period — per-tid counts are noisy.
        A thread on-CPU for the full window may still produce 0 samples. Treat absences as UNKNOWN.
  tid=2471485: 3 samples ≈ 30ms on-CPU (100% of the poll)
        spaces::simple_avx::dot_similarity_avx < DenseVectorStorageImpl<f32>>::score_internal < GraphLayersBuilder>::link_with_heuristic::{closure#0}> < graph_layers_builder::GraphLayersBuilder>::link_new_point < rngs::thread::ThreadRng> < rngs::thread::ThreadRng>
  tid=2471407: 2 samples ≈ 20ms on-CPU (100% of the poll)
        Vec<u8>>::collect_seq::<&alloc::vec::Vec<f32>> < PointStructPersisted::serialize::<&mut serde_cbor::ser::Serializer<&mut alloc::vec::Vec<u8>>> < Vec<u8>>::collect_seq::<&alloc::vec::Vec<shard::operations::point_ops::PointStructPersisted>> < PointInsertOperationsInternal::serialize::<&mut serde_cbor::ser::Serializer<&mut alloc::vec::Vec<u8>>> < PointOperations::serialize::<serde::private::ser::FlatMapSerializer<serde_cbor::ser::CollectionSerializer<&mut alloc::vec::Vec<u8>>>> < OperationWithClockTag::serialize::<&mut serde_cbor::ser::Serializer<&mut alloc::vec::Vec<u8>>>
  tid=2471418: 2 samples ≈ 20ms on-CPU (100% of the poll)
        0x7fffcef7ebb2 < __clock_gettime < recorder::runtime_context::make_worker_unpark < recorder::register_hooks::{closure#5} < worker::Context>::run < run::{closure#0}::{closure#0},()>
  tid=2471415: 1 samples ≈ 10ms on-CPU (66% of the poll)
        codec::buffer::DecodeBuf> < codec::buffer::DecodeBuf> < codec::buffer::DecodeBuf> < Vector::merge_field::<&mut &mut tonic::codec::buffer::DecodeBuf> < codec::buffer::DecodeBuf> < codec::buffer::DecodeBuf>
  tid=2471486: 1 samples ≈ 10ms on-CPU (66% of the poll)
        range::Range<usize>>>::{closure#0}>> < ops::range::Range<usize>>> < mmap::MmapFile>>::put_value < PayloadStorageEnum::overwrite < StructPayloadIndex::overwrite_payload < segment::Segment>::write_point_parts

══════════════════════════════════════════════════════════════════════
LONG POLL  task=89  worker=13  dur=13.59ms (13× this runtime's p99)  [+3456.41 .. +3470.00]ms
spawn: lib/collection/src/update_handler.rs:197:58

CLASSIFICATION: OFF-CPU (23 sched samples) — the worker was descheduled by the kernel inside this poll.
Off-CPU sched stacks ARE present — read them directly (this is the blocking syscall/lock):
  6× fsync < fs::File>::sync_all < path::PathBuf>::{closure#0}> < <&std::path::PathBuf> < SegmentsManifest>::sync::{closure#0}> < segment_manifest::SegmentsManifest>::sync < segment_holder::SegmentHolder>::sync_segment_manifest < UpdateWorkers>::optimization_worker_fn::{closure#0} < handle::Handle>>>::poll < handle::Handle>>>::poll < worker::Context>::run_task < worker::Context>::run
  6× fsync < fs::File>::sync_all < atomicwrites::imp::replace_atomic < path::PathBuf>::{closure#0}> < <&std::path::PathBuf> < SegmentsManifest>::sync::{closure#0}> < segment_manifest::SegmentsManifest>::sync < segment_holder::SegmentHolder>::sync_segment_manifest < UpdateWorkers>::optimization_worker_fn::{closure#0} < handle::Handle>>>::poll < handle::Handle>>>::poll < worker::Context>::run_task
  3× mkdir < imp::unix::create < path::PathBuf>::{closure#0}> < path::PathBuf>::{closure#0}> < <&std::path::PathBuf> < SegmentsManifest>::sync::{closure#0}> < segment_manifest::SegmentsManifest>::sync < segment_holder::SegmentHolder>::sync_segment_manifest < UpdateWorkers>::optimization_worker_fn::{closure#0} < handle::Handle>>>::poll < handle::Handle>>>::poll < worker::Context>::run_task

WHO ELSE WAS ON-CPU during this 14ms (per-tid census; ~10.1ms on-CPU per sample at 99Hz; expected ≤1.3 samp/thread):
  NOTE: poll is short relative to the sampling period — per-tid counts are noisy.
        A thread on-CPU for the full window may still produce 0 samples. Treat absences as UNKNOWN.
  tid=2471493: 3 samples ≈ 30ms on-CPU (100% of the poll)
        spaces::simple_avx::dot_similarity_avx < DenseVectorStorageImpl<f32>>::score_stored_batch::{closure#0}> < DenseVectorStorageImpl<f32>>>::score_points < graph_layers_builder::GraphLayersBuilder>::link_new_point < {closure#13}::call_mut < OperationError>>::consume_iter::<core::iter::adapters::map::Map<rayon::vec::SliceDrain<u32>, &<segment::index::hnsw_index::hnsw::HNSWIndex>::build<rand::rngs::thread::ThreadRng>::{closure#13}>>
  tid=2471496: 3 samples ≈ 30ms on-CPU (100% of the poll)
        {closure#0}>::{closure#0}>::{closure#0}> < GraphLayersBuilder>::link_with_heuristic::{closure#0}> < graph_layers_builder::GraphLayersBuilder>::link_new_point < {closure#13}::call_mut < OperationError>>::consume_iter::<core::iter::adapters::map::Map<rayon::vec::SliceDrain<u32>, &<segment::index::hnsw_index::hnsw::HNSWIndex>::build<rand::rngs::thread::ThreadRng>::{closure#13}>> < thread::ThreadRng>::{closure#13}>>
  tid=2471495: 3 samples ≈ 30ms on-CPU (100% of the poll)
        graph_layers_builder::GraphLayersBuilder>::link_new_point < {closure#13}::call_mut < OperationError>>::consume_iter::<core::iter::adapters::map::Map<rayon::vec::SliceDrain<u32>, &<segment::index::hnsw_index::hnsw::HNSWIndex>::build<rand::rngs::thread::ThreadRng>::{closure#13}>> < thread::ThreadRng>::{closure#13}>> < OperationError>>, <()::default>, <segment::index::hnsw_index::hnsw::HNSWIndex>::build<rand::rngs::thread::ThreadRng>::{closure#13}>>::{closure#1}>::{closure#0}, core::result::Result<(), segment::common::operation_error::OperationError>>>::run_inline < operation_error::OperationError>>::{closure#0}
  tid=2471497: 3 samples ≈ 30ms on-CPU (100% of the poll)
        DotProductMetric::similarity < DenseVectorStorageImpl<f32>>::score_internal < {closure#0}>::{closure#0}>::{closure#0}> < GraphLayersBuilder>::link_with_heuristic::{closure#0}> < graph_layers_builder::GraphLayersBuilder>::link_new_point < {closure#13}::call_mut
  tid=2471499: 2 samples ≈ 20ms on-CPU (100% of the poll)
        spaces::simple_avx::dot_similarity_avx < DenseVectorStorageImpl<f32>>::score_stored_batch::{closure#0}> < DenseVectorStorageImpl<f32>>>::score_points < graph_layers_builder::GraphLayersBuilder>::link_new_point < {closure#13}::call_mut < OperationError>>::consume_iter::<core::iter::adapters::map::Map<rayon::vec::SliceDrain<u32>, &<segment::index::hnsw_index::hnsw::HNSWIndex>::build<rand::rngs::thread::ThreadRng>::{closure#13}>>

══════════════════════════════════════════════════════════════════════
LONG POLL  task=90  worker=13  dur=12.26ms (12× this runtime's p99)  [+4106.19 .. +4118.45]ms
spawn: lib/collection/src/update_handler.rs:229:55

CLASSIFICATION: OFF-CPU (8 sched samples) — the worker was descheduled by the kernel inside this poll.
Off-CPU sched stacks ARE present — read them directly (this is the blocking syscall/lock):
  3× fsync < fs::File>::sync_all < path::PathBuf>::{closure#0}> < AppliedSeqHandler>::save::{closure#0}> < UpdateWorkers>::update_worker_fn::{closure#0} < handle::Handle>>>::poll < handle::Handle>>>::poll < worker::Context>::run_task < worker::Context>::run < run::{closure#0}::{closure#0},()> < worker::run::{closure#0},()> < multi_thread::worker::run
  2× syscall < operations::types::CollectionError>> < UpdateWorkers>::update_worker_fn::{closure#0} < handle::Handle>>>::poll < handle::Handle>>>::poll < worker::Context>::run_task < worker::Context>::run < run::{closure#0}::{closure#0},()> < worker::run::{closure#0},()> < multi_thread::worker::run < {closure#0}>::poll < schedule::BlockingSchedule>>::poll
  2× fsync < fs::File>::sync_all < atomicwrites::imp::replace_atomic < path::PathBuf>::{closure#0}> < AppliedSeqHandler>::save::{closure#0}> < UpdateWorkers>::update_worker_fn::{closure#0} < handle::Handle>>>::poll < handle::Handle>>>::poll < worker::Context>::run_task < worker::Context>::run < run::{closure#0}::{closure#0},()> < worker::run::{closure#0},()>

WHO ELSE WAS ON-CPU during this 12ms (per-tid census; ~10.1ms on-CPU per sample at 99Hz; expected ≤1.2 samp/thread):
  NOTE: poll is short relative to the sampling period — per-tid counts are noisy.
```

</details>
