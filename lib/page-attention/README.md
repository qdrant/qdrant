# Page attention

Vendored from the local `kv-search` prototype, branch `ra-store-compare`.
The format contract is `store/storage/PAGES.md` in that repository.

`pagekernel.rs` retains the original SIMD and scalar kernels and their tests.
`pages.rs` retains the codec, generation writer/loader and graph reader, without
the token graph builder or session/server integration. `pagesearch.rs` retains
the page beam, coarse tail and original-row rescoring. Tail/weight settings are
passed as `Parameters`. `index.rs` contains only the walk helpers, epoch scratch,
dot product and deterministic RNG. `tq4.rs` contains the original codebook and
session rotation. This crate has no Qdrant or protobuf types.
`rescore.rs` preserves the prototype's SIMD accumulation/reduction order on
decoded originals, avoiding scalar-versus-SIMD rounding drift during rescoring.
The adapter issues best-effort IO/CPU prefetch hints for the selected original
rows before rescoring; it never prefetches the whole originals mapping.

The segment adapter imports the six files of one head into its own index
directory and validates identity token-position IDs. The typed REST API is
`POST /collections/{name}/attention` with JSON
`{"query":42,"using":"l0000h0000","ef":64,"rescore":false,"return_top_k":16}`.
`query` is a head_dim Q vector or a point ID (the stored K is then used as Q).
`ef` defaults to 16, rescore to false and return_top_k to 0. The response's
`result` contains `attention`, `lse`, and optional `token_ids`, omitted when
return_top_k is zero. `/attention/batch` accepts `{"queries":[...]}` with up to
64 requests and returns an ordered result array. Returned IDs can be fetched
through ordinary point retrieval; each stored vector concatenates K and V.

Top-k is diagnostic: it ranks tokens in scanned pages, including sinks, using
the scores after any requested original-row rescoring. It does not change the
attention output, which includes an aggregate approximation of unvisited
tokens. No exact full-scan or top-k-subtracted residual mode is exposed.
The API supports one active local replica/shard and one populated immutable
segment. Request authorization, strict-mode limits, rate limits and timeout
checks apply. An individual kernel currently only observes cancellation after
it returns, and hardware counters are not instrumented for the kernel.

The old ordinary-search adapter remains available for regression comparisons:
coordinate IDs `0..head_dim-1` carry output scores, and ID `head_dim` carries
LSE. The typed API does not use that channel or Qdrant's top-k merge pipeline.

The demo adapter parallelizes query heads in Rayon's global pool. Qdrant's
`max_search_threads` does not configure that pool; set `RAYON_NUM_THREADS`
explicitly when enforcing a worker budget. The comparison harness sets both
limits to 12.

## Offline preparation

The default library contains the immutable page-attention read path. Enable the
`builder` feature for offline construction; the server does not need this feature.

```bash
# Linux / WSL
cargo test --locked -p page-attention --features builder --lib
cargo build --locked -p page-attention --features builder \
  --bin page-attention-prepare --profile perf
```

`page-attention-prepare INPUT OUTPUT SESSION LAYER HEAD DIM THREADS` builds one
KV head. `INPUT` contains row-major little-endian data:

| File | Shape / representation |
| --- | --- |
| `keys.bf16` | `[tokens, dim]`, BF16 bits |
| `values.bf16` | `[tokens, dim]`, BF16 bits |
| `queries.bf16` | `[training_queries, dim]`, BF16 bits |
| `positions.u32` | one causal token position per training query |

`DIM` must be 128 or 256; the attention score-channel protocol requires more
tokens than dimensions. `OUTPUT` must not exist. The utility writes and reloads
the six `lLLLLhHHHH.*` files to validate the persisted representation. Failed
output is not a published generation and must not be imported.

The builder preserves the prototype algorithm: centered-key HNSW (M=16,
M0=32, ef_construct=100, min_links=8), causal exact-top-128 miss repair at
ef=256 with at most four added links per node, block-TQ4 navigation, learned
co-visited packing, and shifted page levels. Edge repair uses all supplied Q;
packing uses up to 256 evenly sampled Q, matching the prototype's saved training
sample. Graph insertion is sequential and seeded with 1; independent heads and
the edge-repair/encoding work can run concurrently. Old parallel-insertion
generations are not expected to be byte-identical to a fresh build.

Use `scripts/qdrant_pages_ingest.py` from the `qdrant-pages-demo` branch of
kv-search for the complete workflow. It reads the recorded K/V/Q prefill cache,
invokes this binary (locally or inside the Docker image), publishes all heads
under `pages/`, writes `manifest.json` and its `pages/source.fnv` binding, uploads
original K/V to Qdrant, and waits for indexing. It also verifies held-out queries
against exact full-context attention. No prototype service, model weights, GPU,
prebuilt snapshot, or external source patch is required for these steps.

The `page_attention.generation` path is resolved by the Qdrant server. Mount or
copy the generation and its parent manifest into that filesystem before enabling
indexing. Qdrant imports the files into its own storage. Serving remains immutable,
single-shard, and uses the custom attention score-channel protocol.
