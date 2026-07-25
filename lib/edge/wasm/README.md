# `edge-wasm` — a read-only edge shard on WASI

Compiles the read-only edge-shard query path to `wasm32-wasip2` and serves searches directly out of
object storage, with no Qdrant server in the loop.

```
   wasm32-wasip2 guest                          S3 / GCS / any HTTP object store
  ┌──────────────────────┐                     ┌────────────────────────────────┐
  │ object_store::list ──┼── blocking GET ────►│ GET /?list-type=2&prefix=…      │
  │ object_store::get  ──┼── blocking GET ────►│ GET /<key>                      │
  │                      │                     └────────────────────────────────┘
  │   MemFs  (linear memory)
  │     │
  │     ▼
  │   ReadOnlyEdgeShard<MemFile> ── search / scroll / info
  └──────────────────────┘
```

## Why WASI, and not the browser

`UniversalRead` — the interface every segment component reads through — is **synchronous**. WASI
gives the guest synchronous sockets and a synchronous filesystem, so that interface is satisfiable
directly: no async runtime, no threads, and none of the sync/async bridging `io_bridge` does for the
native blob backends.

A browser cannot do this. There `fetch` only resolves when the JS event loop runs, so blocking the
calling thread deadlocks; the read path would have to move into a Web Worker blocking on
`Atomics.wait`, or onto OPFS sync access handles. That is separate work — this crate deliberately
stops at the target where the existing synchronous interface already fits.

Nothing in the crate is `cfg`-gated on the target: the same code runs natively, which is what the
round-trip test and a native `cargo run` exercise.

## Layout

| File | Role |
|------|------|
| `src/mem_fs.rs` | `MemFs`/`MemFile`: an in-memory `UniversalRead` backend |
| `src/object_store.rs` | Blocking `ListObjectsV2` + object GET over `ureq` |
| `src/shard.rs` | Open, search, scroll, JSON rendering |
| `src/main.rs` | CLI, runnable natively and under a WASI runtime |
| `s3_stub.py` | A local read-only S3-compatible server, for trying it out |

## Try it

```sh
# a real shard: 256 points, 4-dim, named vector "demo", payload field "parity"
cargo run -p edge-wasm --example make_fixture -- /tmp/bucket/collection/0
python3 lib/edge/wasm/s3_stub.py /tmp/bucket 9000 &

# natively
cargo run -p edge-wasm -- http://localhost:9000 collection/0 \
    search --vector 10,10.01,10.02,10.03 --vector-name demo --with-payload

# as a wasm component
cargo build -p edge-wasm --target wasm32-wasip2 --release
wasmtime -S inherit-network -S allow-ip-name-lookup \
    target/wasm32-wasip2/release/edge-wasm.wasm \
    http://localhost:9000 collection/0 \
    search --vector 10,10.01,10.02,10.03 --vector-name demo --with-payload
```

Against a real bucket the only requirement is anonymous read access. `ureq` is built with default
features off, so there is no TLS backend and this speaks `http://` only; adding one, or presigned
URLs, touches nothing outside `object_store.rs`.

## What works, and what does not

Verified end-to-end: shard open from a segment manifest, vector search with exact-match scoring,
filtered search, scroll, payload retrieval and `info` — natively and under `wasmtime`.
`tests/mem_fs_round_trip.rs` covers the same path without a network, by writing a real shard with
the ordinary read-write path and reading it back through `MemFs`.

Known limits of this target:

- **Single-threaded.** Plain `wasm32-wasip2` cannot spawn threads, so the shard's rayon pool is
  built with `use_current_thread()` and one worker. Per-segment reads run serially.
  `wasm32-wasip1-threads` would lift this.
- **Scalar distance kernels.** The SSE/AVX/NEON paths in `quantization` and `segment::spaces` are
  architecture-gated with scalar fallbacks; no `simd128` implementation exists yet.
- **Whole objects only.** Vector chunks and Gridstore pages are *pre-allocated*: a 256-point shard
  has 32 MiB of mostly-zero `chunk_0.mmap` and `page_0.dat`. On a local filesystem those are sparse
  files (164 KiB actual), but an object store stores — and this client downloads — the full extent.
  The leader's `wal/` is skipped for the same reason, and because a follower never replays it.

  This is a property of the preload, not of the target. Because reads already block, a backend
  issuing an HTTP Range request per read drops into the same `UniversalRead` slot and removes both
  the up-front download and the resident-set ceiling. That is the obvious next step.
- **≤ 4 GiB.** 32-bit address space, so that is the hard ceiling on a shard's resident size.
- **No mmap, no WAL, no writes.** `memmap2` selects its `stub` backend on any non-unix, non-windows
  target, where every mapping call returns `ErrorKind::Unsupported`; the constructors in
  `common::mmap` return the same there, and `lib/wal` compiles but cannot open a segment. The
  read-only path touches none of it.
