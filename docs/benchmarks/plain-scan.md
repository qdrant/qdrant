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

## In-process concurrency diagnostic

Passing `--concurrency` switches to a closed-loop request driver with a fixed
search worker pool. Each request executes one `BatchFilteredSearcher` search over
the same immutable dataset. `B` is the number of queries inside one request;
`C` is the number of independent outstanding requests. Separate B1 requests are
never combined into one larger batch. This measures the scanner and its request
queue; it does not reproduce Qdrant HTTP, its server scheduler, HNSW, sharding,
or `max_search_threads` within a query.

Run these two fixed-axis sweeps with the same three-worker budget and CPU mask:

```sh
# Concurrency sweep: B1, C1/4/16/32/64/128.
taskset -c 2,3,4 target/release/deps/sq8_full_scan-<hash> \
  --batches 1 --concurrency 1,4,16,32,64,128 --search-threads 3 \
  --duration-secs 10 --samples 1 --output concurrency-b1.json

# Batch sweep: C4, B1/4/16/32/64/128.
taskset -c 2,3,4 target/release/deps/sq8_full_scan-<hash> \
  --batches 1,4,16,32,64,128 --concurrency 4 --search-threads 3 \
  --duration-secs 10 --samples 1 --output batches-c4.json
```

Select three allowed logical CPUs, preferably on three physical cores, and record
their topology. All worker threads and the lightweight coordinator inherit this
affinity mask. C128 still creates only three search workers, plus the coordinator.
Use one sample for initial screening; repeat baseline and candidate runs in
alternating order before drawing performance conclusions. Without `--concurrency`,
the existing single-thread B1/B8/B64 microbenchmark is unchanged. With multiple
values for both flags, the requested cases are their Cartesian product.

The concurrency fixture uses a deterministic pool of at least 128 queries
(`--query-pool`, enlarged to fit the largest batch). Request IDs select successive
pool offsets and each batch wraps around the pool. Both sweeps therefore use the
same query distribution. Independent B1 reference searches and per-worker warmup
run before timing. The warmed workers prepare a fresh scorer for every request;
query encoding is included in request service time.

The coordinator initially submits C requests, then replaces each completed
request in the same client slot until the requested duration expires. It stops
submitting and drains every outstanding request. Queue capacity and total
in-flight work are bounded by C. Initial clients arrive at the test start, so
their queue latency also includes initial dispatch. Every measured result is
retained and checked
against its ordered reference IDs and exact score bits after timing stops; a
mismatch fails the run. The reference results and raw request timestamps are
included in JSON. Result retention and coordinator/channel overhead are part of
this diagnostic; percentile calculation, result validation, thread teardown and
JSON serialization are outside timing.

Per-case retention uses a conservative estimate of result capacities and latency
record sizes, limited by `--result-budget-mib` (64 MiB by default, excluding the
dataset, earlier samples' latency records and worker scratch space). If the
budget is exhausted, admissions stop, workers
drain, and the run fails instead of emitting a truncated success report. Shorten
the interval or raise the budget for unusually fast fixtures or large top-k.
Worker search failures also fail the run rather than leaving an outstanding
request unaccounted for.

The main throughput fields divide all successful requests and logical queries by
the **actual elapsed time including drain**. Logical query/s equals request/s
times B. A separate `requests_per_second_in_window` counts only responses received
within the requested submission window and divides by that window's duration.
Reports also include the drain duration and count completed within the window.
At high C, drain can be substantial; never divide all drained requests by only
the configured ten seconds.

Latency runs from submission before enqueue to receipt by the coordinator. It
includes queueing, scorer preparation, scanning, top-k construction and response
delivery. The report preserves queue/service times and all four timestamps for
each request; p50/p95/p99 are nearest-rank percentiles over the full submitted
cohort, including requests completed during drain. This is a closed-loop workload,
so its latency distribution should not be presented as an open-loop arrival test.
Worker CPU cycles are not collected in this mode; `--require-cycles` is rejected
because the microbenchmark's main-thread counter would omit search worker cycles.

For a short protocol and correctness check, use the same sweep flags with
`--points 257 --dim 33 --duration-secs 0.05 --samples 1 --warmup 1`. Such a small
fixture primarily measures the driver and is not performance evidence.
