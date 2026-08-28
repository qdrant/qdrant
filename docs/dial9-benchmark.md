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
