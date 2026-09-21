# Page attention read path

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
directory, validates identity IDs, and returns coordinate IDs `0..head_dim-1`
with values in the scores, followed by ID `head_dim` carrying the LSE. It sorts
by score for Qdrant's merge pipeline; consumers must reconstruct by ID.

The demo adapter parallelizes query heads in Rayon's global pool. Qdrant's
`max_search_threads` does not configure that pool; set `RAYON_NUM_THREADS`
explicitly when enforcing a worker budget. The comparison harness sets both
limits to 12.

Build and test **inside WSL only**:

```sh
cargo test -p page-attention --lib
cargo check -p segment
```

The generation builder stays in the prototype. The adapter requires its source
manifest and `pages/source.fnv` at import time; subsequent opens use the copied
files and the rotation session ID persisted in segment configuration.
