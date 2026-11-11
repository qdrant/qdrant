# Batched SQ8 plain-scan benchmark

`sq8_full_scan` measures `BatchFilteredSearcher` on a deterministic in-memory
SQ8 fixture: one million normalized 512-dimensional vectors, dot distance,
Top10, and batch sizes 1, 8, and 64. It covers the unfiltered
`peek_top_visible` driver and `peek_top_iter` with a synthetic filter accepting
90% of IDs. Both drivers include scoring and top-k selection. There is no HNSW,
HTTP, RPC, FP32 reranking, or concurrent query scheduler.

Build once, then pin the executable to one available logical CPU on Linux:

```sh
cargo bench -p segment --bench sq8_full_scan --no-run
taskset -c 2 target/release/deps/sq8_full_scan-<hash> --output baseline.json
```

Use the executable path printed by Cargo. CPU 2 is only an example: select a CPU
inside the host's allowed affinity mask. Keep the CPU, toolchain, build flags,
fixture seed, and command identical for every implementation being compared.
Record the commit, CPU model, and virtualization status alongside each report.
Run the baseline and candidate repeatedly in alternating order; report all samples.
The benchmark alternates case order within each run, but does not control other
processes, CPU frequency, or physical-core isolation.

Each case is checked against independent B1 searches before measurement, comparing
ordered IDs and score bits within that build. The report also saves the B1 ordered
IDs and score bits: compare `b1_reference_results` between baseline and candidate
reports to check cross-build equality. Within-build checks alone cannot detect a
regression shared by both B1 and batched searches.

Dataset construction, query preprocessing, correctness
checks, warmup, and JSON output are outside the measured region. The timed region
includes scan-local scratch allocation, scoring, top-k selection, and construction
of the result vectors. Each sample separately records `query_prepare_ns` for
searcher construction, including query encoding and any backend-specific packing.
The output reports every sample's scan query/s and ns per
**scored query-point pair**. Filtered scans also pay the cost of checking excluded
IDs, but their denominator counts only the points actually scored.

On Linux a pinned `perf_event_open` hardware counter records user-mode CPU cycles
for the calling thread, enabled only around the timed region. Multiplexed or
unscheduled counters fail the run. If access is denied or the PMU is unavailable,
cycle fields are `null`; no frequency or TSC-based cycle estimate is substituted.
Use `--require-cycles` on a measurement host where hardware counters are available
to require cycles per query-point pair. The benchmark never changes system policy.

For a quick correctness smoke check (not a performance baseline):

```sh
cargo bench -p segment --bench sq8_full_scan -- \
  --points 257 --dim 33 --batches 1,8,64 --samples 1 --warmup 0
```

`--filter-percent`, `--top`, `--points`, `--dim`, `--batches`, `--samples`,
`--warmup`, and `--seed` are configurable. Keep the default one-million-point
workload for comparisons attached to #11462–#11465. The synthetic filter measures
the filtered scan driver, not payload-index construction or filter planning.
