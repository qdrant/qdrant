# `edge-wasm` — a read-only edge shard in the browser

Compiles the read-only edge-shard query path to `wasm32-unknown-unknown` and serves searches
directly out of object storage, with no Qdrant server in the loop.

```
       browser                                    S3 / GCS / any HTTP object store
  ┌──────────────────────┐                       ┌────────────────────────────────┐
  │ open(base, prefix) ──┼── fetch: list ───────►│ GET /?list-type=2&prefix=…      │
  │                    ──┼── fetch: get × N ────►│ GET /<key>                      │
  │                      │                       └────────────────────────────────┘
  │   MemFs  (linear memory)                        ▲ async — happens once, at open
  │     │                                           │
  │     ▼                                           │
  │   ReadOnlyEdgeShard<MemFile> ── search/scroll ──┘ sync — every read is a slice
  └──────────────────────┘
```

## Why it is split at that line

[`UniversalRead`][universal-read] — the interface every segment component reads through — is
**synchronous**. The native blob backends satisfy that by blocking a thread on a Tokio runtime
(`io_bridge`), which a browser cannot do: `fetch` only resolves when the JS event loop runs, so
blocking the thread that would run it deadlocks.

So the fetching is hoisted out of the read path entirely. `open` is `async`: it lists the prefix,
downloads every object, and parks the bytes in `MemFs`. From that point on every read is a slice of
linear memory, and the synchronous interface is honest.

The cost is that the whole shard is resident. Lazy range reads would need either a Web Worker
blocking on `Atomics.wait` against a fetching main thread, or OPFS sync access handles — both keep
this same `UniversalRead` impl and only change where the bytes come from.

[universal-read]: ../../common/common/src/universal_io/traits/read.rs

## Layout

| File | Role |
|------|------|
| `src/mem_fs.rs` | `MemFs`/`MemFile`: the in-memory `UniversalRead` backend |
| `src/shard.rs` | Target-independent core — open, search, scroll, JSON rendering |
| `src/object_store.rs` | `fetch`-based `ListObjectsV2` + object GET (wasm only) |
| `src/lib.rs` | The `wasm-bindgen` surface (wasm only) |
| `demo/` | A browser page and a local S3-compatible stub to point it at |

## Build

```sh
cargo build -p edge-wasm --target wasm32-unknown-unknown --release
wasm-bindgen --target web --out-dir lib/edge/wasm/demo/pkg \
    target/wasm32-unknown-unknown/release/edge_wasm.wasm
```

The `.wasm` is ~19 MB unoptimised; `wasm-opt -Oz` and stripping the unused index kinds cut that
substantially, and neither was attempted here.

## Try it locally

```sh
# 1. write a real shard (256 points, 4-dim, named vector "demo", payload field "parity")
cargo run -p edge-wasm --example make_fixture -- /tmp/bucket/collection/0

# 2. serve it as if it were a bucket, with CORS
python3 lib/edge/wasm/demo/s3_stub.py /tmp/bucket 9000

# 3. build + bind (see above), then serve the demo directory and open index.html
python3 -m http.server -d lib/edge/wasm/demo 8080
```

The demo defaults to `http://localhost:9000/test-bucket`; point it at
`http://localhost:9000` with prefix `collection/0` to match the stub above.

Against a real bucket the requirements are anonymous read access and CORS headers permitting the
page's origin. Requests are unsigned; presigned URLs would slot into `object_store.rs` the same way.

## What works, and what does not

Verified end-to-end (`tests/mem_fs_round_trip.rs`, plus a Node run against the stub): shard open
from a segment manifest, vector search with exact-match scoring, filtered search, scroll, payload
retrieval, and `info`.

Known limits of this target:

- **Single-threaded.** `wasm32-unknown-unknown` cannot spawn threads, so the shard's rayon pool is
  built with `use_current_thread()` and one worker. Per-segment reads run serially.
- **Scalar distance kernels.** The SSE/AVX/NEON paths in `quantization` and `segment::spaces` are
  architecture-gated with scalar fallbacks; no `simd128` implementation exists yet.
- **Whole objects only.** Vector chunks and Gridstore pages are *pre-allocated*: a 256-point shard
  has 32 MiB of mostly-zero `chunk_0.mmap` and `page_0.dat`. On a local filesystem those are sparse
  files (164 KiB actual), but an object store stores — and this client downloads — the full extent.
  The leader's `wal/` is skipped for the same reason, and because a follower never replays it.
- **≤ 4 GiB.** 32-bit address space, so that is the hard ceiling on a shard's resident size.
- **No mmap, no WAL, no writes.** `memmap2` has no wasm backend at all; the mapping constructors in
  `common::mmap` return `ErrorKind::Unsupported` there, and `lib/wal` compiles but cannot open a
  segment. The read-only path touches none of it.
