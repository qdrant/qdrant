# Query profiling (`EXPLAIN` / `EXPLAIN ANALYZE`) — design

Status: beta, behind the `query_profiling` feature flag. Branch `query-profiling`.

## Goal

Someone asking "why is this query slow?" should get the answer from Qdrant itself, at three levels:

| Level | Postgres / MySQL / ArangoDB | Qdrant (this design) |
|---|---|---|
| Plan | `EXPLAIN`, `db._explain` | `?profile=explain`: the strategy each segment *would* use, real cardinality estimates, no vector scored |
| Execution profile | `EXPLAIN (ANALYZE, BUFFERS)`, `EXPLAIN ANALYZE`, `db._profileQuery` | `?profile=analyze`: the response carries a `profile` with phase, shard and segment timings, strategies, counters and notes |
| Workload | `pg_stat_statements`, Performance Schema digests | `GET /profiler/workload`: per-fingerprint calls / total / mean / p50 / p95 / points visited, next to `GET /profiler/slow_requests` |

## What we borrowed, and where Qdrant needs a different shape

- **Postgres EXPLAIN ANALYZE** prints a tree of plan nodes with `actual time=first..total rows=N loops=L`, and with `BUFFERS` it adds
  `shared hit/read`. Borrowed: *estimate next to actual* (`estimated_cardinality` next to `actual_cardinality`), the
  number of *calls* on every phase (Postgres `loops`), and I/O split by where the data came from (`*_io_read_bytes`,
  `remote_io_cached_read_bytes` vs `remote_io_non_cached_read_bytes`, like `hit` vs `read`).
- **MySQL EXPLAIN ANALYZE** (`FORMAT=TREE`) puts the *access method* first on every line ("Index lookup on t using idx",
  "Table scan on t"). Borrowed: every segment states its `strategy` (`hnsw`, `hnsw_filtered`, `acorn`,
  `payload_index_first`, `full_scan`, `exact`, `sparse_index`, `indexed_read`, `full_scan_read`), **and why it was chosen**
  (`strategy_reason`, e.g. `cardinality_below_threshold` with the `full_scan_threshold` used). Qdrant's planner is
  adaptive per segment, so the reason matters more than in MySQL.
- **ArangoDB `db._profileQuery`** prints a fixed "Query Profile" table of phases (parsing, optimizing, executing,
  finalizing), a per-node table with `Calls / Items / Runtime`, and a list of applied optimizer rules. Borrowed: the
  flat list of named *phases* with `time_ms` and `calls`, and human-readable `notes` at the end (Arango's optimizer rules,
  our derived findings: "`color` has no payload index", "2 segments are not indexed yet").

**Why Qdrant can't use a plan tree.** A Postgres plan is one tree executed once. A Qdrant query is the
*same* small plan (prefetch → search → fusion/rescore → merge) run independently in every **segment** of every
**shard**, and each segment picks its own strategy from its own size, index state and cardinality estimate. A
200-segment collection with one plan tree per segment would be unreadable, and the interesting fact is almost always
"*these* segments chose differently". So the shape is:

```
QueryProfile                       request on the receiving node
├── phases[]                       coordinator: shards, merge, collection_rescore, payload_retrieval
├── shards[]                       one per shard operation, local or remote (with peer_id, network_time_ms)
│   ├── phases[]                   shard: prefetch, segment_search, merge, rescore, fusion, payload_retrieval
│   └── segments[]                 one per segment × search group: strategy, reason, filter, counters
└── notes[]                        findings derived from all of the above
```

Phases are aggregated by name with `calls`, not nested: prefetch trees are recursive in Qdrant and the per-node
breakdown Postgres gives is less useful than "how much time went into segment search vs. rescoring".

## API

Opt-in URL parameter (REST) / field (gRPC), on search, search batch, query, query batch, scroll, count, facet:

```
POST /collections/{name}/points/query?profile=analyze     # run it and report
POST /collections/{name}/points/query?profile=explain     # plan only, empty result
```

```protobuf
message QueryPoints { ... optional ProfileMode profile = 16; }
message QueryResponse { ... optional QueryProfile profile = 4; }
```

- Without the flag enabled, `profile=...` is rejected with 400 and an explanation; requests without it are unaffected.
- The profile is also returned on errors (e.g. timeout), to show how far the request got.
- **Explain** runs everything up to the per-segment strategy choice (including cardinality estimation and sampling,
  which are real), then returns before scoring vectors or reading points. It is forwarded to remote shards. Stages
  that consume prefetch results (rescoring, fusion, grouping, order_by) have no candidates in explain mode; the
  profile says so in `notes`. Explain never returns points, whatever the request:
  - unfiltered scroll, order_by, random sampling and exact count return before reading (`strategy: unfiltered_read`);
  - BM25 text leaves are not run (they have no strategy to choose) and add a note saying so;
  - an older peer that ignores the mode (mixed-version cluster) returns no profile: its results are **discarded**
    and a note names the peer;
  - approximate count (`exact: false`) still returns its estimate: estimating *is* its plan, nothing is read. From
    an older peer, its (also estimated) count is kept, so a mixed-version cluster doesn't under-count;
  - queries that reference points by id (recommend, `nearest` by id) still fetch those points' stored vectors to
    build the query: that read is the input of the plan, not its execution;
  - `order_by` without a range index fails like the request would, instead of reporting a plan that can't run.
- Explain is rate limited like any read. Searches and query batches cost one unit per planned leaf (search, read or
  BM25 leaf): planning (cardinality estimation, sampling) is real work, scoring is skipped. Operations that do the
  same work in explain mode cost the same: `retrieve` (e.g. the vectors of recommend examples), scroll and count.
- A peer with `query_profiling` disabled still serves profiled internal requests (the coordinator checked the
  flag), but doesn't count distances, which would cost every request of that process: its profile says so in a
  note, and its distance counts show as 0.
- Unknown enum values from a newer peer (`strategy`, `strategy_reason`) are reported as absent, not as an invalid
  profile, so new values can be added within schema version 1.
- gRPC batches take `profile` on the batch request; `profile` on a batch item is rejected rather than ignored.
- **Explain on facet is rejected** with a 400 that says why: a facet has no strategy to choose — it always reads one
  payload index and counts. Explain on count/scroll is supported (it shows indexed vs. full-scan reads).

Workload:

```
GET    /profiler/workload?limit=20&collection=<name>&sort=total_time|mean_time|p95_time|calls|vector_scores
DELETE /profiler/workload          # reset, like pg_stat_statements_reset()
```

## Output schema (version 1) — annotated example

Real output (debug build, 20k points, collection still indexing), trimmed to one segment:

```jsonc
{
  "result": { "points": [ ... ] },
  "status": "ok", "time": 0.020,
  "profile": {
    "version": 1,                         // bumped on any non-additive change
    "mode": "analyze",
    "total_time_ms": 20.01,               // wall time on the receiving node
    "phases": [                           // coordinator phases, summed over calls
      { "name": "shards", "time_ms": 19.48, "calls": 1 },
      { "name": "merge", "time_ms": 0.09, "calls": 1 },
      { "name": "collection_rescore", "time_ms": 0.01, "calls": 1 }
    ],
    "shards": [{
      "shard_id": 0,
      "peer_id": null,                    // set in distributed mode
      "location": "local",                // or "remote"
      "operation": "query_batch",
      "time_ms": 18.60,                   // on the peer that executed it
      "network_time_ms": null,            // remote: round trip minus time_ms
      "phases": [
        { "name": "prefetch", "time_ms": 18.47, "calls": 1 },
        { "name": "segment_search", "time_ms": 16.82, "calls": 1 },
        { "name": "merge", "time_ms": 0.14, "calls": 1 }
      ],
      "segments": [{
        "segment_id": "e7bba57a-…",
        "appendable": false,
        "indexed": true,                  // false = brute force during indexing
        "points": 15000,
        "operation": "search",            // or "read" (scroll/count/facet)
        "vector_name": "",
        "batch_size": 1,
        "limit": 5,
        "strategy": "payload_index_first",
        "strategy_reason": "cardinality_below_threshold",
        "full_scan_threshold": 400,       // in points, as used for the decision
        "filter": {
          "estimated_cardinality": { "min": 0, "exp": 248, "max": 331 },
          "actual_cardinality": null,     // filled when known for free (exact count, full scan)
          "indexed_keys": ["city"],       // primary clauses served by a payload index
          "unindexed_keys": ["color", "price"],   // checked by reading payload
          "full_payload_scan": false      // true = no index narrowed candidates at all
        },
        "time_ms": 15.72,
        "counters": {
          "vector_scores": 224,           // distance computations, original vectors
          "quantized_vector_scores": 0,   // distance computations, quantized vectors
          "vector_io_read_bytes": 0,      // bytes touched in on-disk vector storage, cached or not
          "payload_io_read_bytes": 26994, // bytes touched in on-disk payload storage, cached or not
          "payload_index_io_read_bytes": 0,
          "remote_io_cached_read_bytes": 0,
          "remote_io_non_cached_read_bytes": 0,
          "major_page_faults": 0,         // mmap accesses that waited for the disk (OS, this thread)
          "disk_read_bytes": 0            // bytes actually read from the block device (OS, this thread)
        }
      }]
    }],
    "notes": [
      "Filter conditions on `color`, `price` have no payload index: they are checked by reading the payload of each candidate point. Consider creating a payload index."
    ]
  }
}
```

Stability: the Rust model (`lib/common/common/src/ambient/profile/model.rs`) is the single source; REST serializes it,
gRPC (`QueryProfile` in `points.proto`) mirrors it field by field via `lib/api/src/grpc/profile_conversions.rs`, and the
tests compare the two. Changes within a version are additive only (new optional fields, new enum values clients must
tolerate). `WORKLOAD_SCHEMA_VERSION` versions the workload endpoint separately.

## Instrumentation points

The profiler lives in the existing per-request **ambient context** (`lib/common/common/src/ambient/`), which already
carries hardware counters across tokio tasks and rayon/search-runtime threads. A profiled request gets a
`ProfileHandle` in its root; nested *frames* collect phases (request), shard profiles (request) and segment profiles
(shard). Shard and segment scopes **fork** the context so their counters are read in isolation while still being
forwarded to the request's totals.

| Layer | Where | What |
|---|---|---|
| API | `src/actix/api/{query,search,count,facet,…}_api.rs`, `src/tonic/api/points_api.rs`, `query_common.rs` | `profile` param, flag check, `RequestHwCounter::new_profiled`, attach `profile` to response |
| Coordinator | `lib/collection/src/collection/query.rs` | phases `shards`, `merge`, `collection_rescore`, `payload_retrieval` |
| Remote shards | `lib/collection/src/shards/remote_shard.rs`, `points_internal_service.proto`, `points_internal_api.rs` | forward mode, time the round trip, graft the remote profile, `network_time_ms` |
| Local shard | `lib/collection/src/shards/local_shard/shard_ops.rs` (`profiled`) | `ShardScope` for query_batch, core_search, scroll, count, facet, retrieve |
| Shard query | `lib/collection/src/shards/local_shard/query.rs` | phases `prefetch`, `rescore`, `payload_retrieval` |
| Segment search | `lib/collection/src/collection_manager/segments_searcher/{search,segment_search}.rs` | `SegmentScope` per segment × search group; phases `segment_search`, `merge` |
| Strategy choice | `lib/segment/src/index/hnsw_index/hnsw/read_view/dispatch.rs`, `plain_vector_index/read_view/search.rs`, `sparse_vector_index/read_view/search.rs` | `note_strategy`, `note_cardinality`, `note_full_scan_threshold`, explain short-circuit |
| Filtered reads | `lib/segment/src/segment/read_view/scroll.rs`, `lib/collection/src/shards/local_shard/{scroll,facet}.rs`, `segments_searcher/retrieve.rs` | segment `read` profiles, indexed vs. full-scan reads, exact counts |
| Distance counts | `lib/segment/src/vector_storage/{query_scorer,quantized}/*` → `HwScale` | `ScoreKind::{Original,Quantized}` → new `HwMetric::{VectorScores,QuantizedVectorScores}` |
| Workload | `lib/collection/src/profiling/workload.rs`, `shard_ops.rs` (`profiled_with_workload`), `src/actix/api/profiler_api.rs` | fingerprint, histogram, endpoints |

**Zero cost when off.** Every profiler entry point is `slot::with_context_unchecked(...)` → `None` → return: one
thread-local read and a branch, at segment/shard/request granularity, never per point. Strings (segment ids, payload
keys) are built in closures only when profiled, and the shard id is parsed only when profiled. Distance counts are
bumped only when the flag is on (a process-wide `AtomicBool` set at startup): with the flag off, scorers bump exactly
the metrics they did before. Flushing hardware counters skips zero metrics, so the two new metrics add no atomic
operations when unused. Workload stats are guarded by a `OnceLock<bool>` read of the flag; the request is cloned
and fingerprinted only when it is set, after the operation, and not for rate-limited calls.

Distance counts per scorer: one per comparison of the query (or of each example, for recommend, discover and
context queries) with a stored vector. Dense scorers count through `HwScale::cpu`; sparse scorers count one per dot
product, separately from their CPU cost, which depends on vector lengths (`HwScale::cpu_units`); multi-vectors count
one per sub-vector pair, original and quantized alike (the quantized MaxSim functions count `query × point`
sub-vectors); quantized scorers count explicitly because their CPU is bumped inside the `quantization` crate;
TurboQuant storages keep only encoded vectors, so their comparisons count as quantized; the filtered sparse fallback
(`plain_search`) counts one per scored point; the sparse inverted index counts the posting-list entries of the lists it
searches, an upper bound (pruning and the filter can skip entries; counting exactly would mean a bump in the innermost
loop). Sparse indexes count as original whatever their weight type (`float16`, `uint8`, compressed `QuantizedU8`), like
dense storages with a smaller datatype; telling them apart would need the weight type in the search context.

`filter.unindexed_keys` lists full payload paths, including keys inside `nested` conditions (`diet[].likes`), as the
payload index is selected for them at query time (`segment::data_types::load_profile::filter_payload_keys`).

## Overhead measurements

Setup: `--profile perf` builds of this branch and of its base commit (`085f92188`), 12-vCPU shared VM, one
collection with 100k × 128d cosine vectors in 4 indexed segments, a keyword index on `city` (100 values), no index on
`color` (3 values). A single client sends requests sequentially over keep-alive HTTP.

Method: sequential runs with server restarts proved useless here. Two identical `base` runs differed by up to 37%.
So each comparison runs **two servers on identical copies of the storage at once**, sends the same request to both,
alternates which goes first, and reports the **median of per-request latency ratios** (1000 pairs; 300 for the slow
workloads, after 50 warm-up pairs). An A/A run (base vs. base) gives the noise floor. Latency is end-to-end client
time, including parsing the (bigger) response in Python. Scripts: `docs/query_profiling/bench.py` (load + sequential runs), `docs/query_profiling/paired.py` (paired A/B).

| Workload (base median) | A/A noise floor | flag off vs. base | flag on, no `profile` (workload stats) vs. base | `profile=analyze` vs. none, same server |
|---|---|---|---|---|
| query, unfiltered (1.5 ms) | +5.1% | −0.7% | +1.4% | **+5.1%** (+69 µs) |
| query, indexed filter (0.6–1.0 ms) | +1.8% | +1.6% | −0.6% | **+8.2%** (+51 µs) |
| query, un-indexed filter (27 ms) | −7.0% | −1.8% | −0.8% | +2.1% |
| scroll, indexed filter (0.5 ms) | −0.6% | −2.0% | −0.7% | **+7.4%** (+42 µs) |
| count, exact, un-indexed filter (60 ms) | +0.9% | −1.4% | +0.1% | +0.5% |

Conclusions:

- **Off: no measurable cost.** All differences are inside the A/A noise floor, in both directions.
- **Workload statistics (flag on): no measurable cost** at these latencies. The fingerprint costs one serialization of
  the shard request plus a mutex-protected map update. Under very high QPS with tiny requests, the global mutex is the first
  thing to shard (see open questions).
- **`profile=analyze`: a fixed 40–70 µs per request.** That meets the 5% target for requests from ~1.5 ms up
  (+0.5% to +5%), but not for sub-millisecond requests (+7–8%). The cost does not scale with the work done: it is
  building, serializing and parsing the profile (about 3 KB of JSON for 4 segments), plus one context fork per shard
  and segment. It is opt-in per request, so it never affects requests that don't ask for it. If needed, the cheapest
  wins are skipping empty segments in the output and not cloning segment ids.

Note: these measurements were taken before the review fixes below. The fixes make the flag-off path do strictly
less work (score counting gated, zero metrics skipped on flush, no eager clone/parse), so the "off" column is an
upper bound; `analyze` was not re-measured.

## Review

A Codex review of the branch found 10 issues, all confirmed and fixed with tests: explain returning or computing
results (older peers, unfiltered reads, BM25), quantized and sparse-index scores not counted, per-item `profile` in
gRPC batches silently ignored, unbounded size of stored fingerprints, flag-off overhead in scorers and shard ops,
fingerprinting before the rate limiter, explain consuming the full read budget, and forks detaching trace events
from the request span.

## Real disk I/O (page-cache misses)

The `*_io_read_bytes` hardware counters count bytes *touched* in on-disk (mmap) storage, whether the pages were cached
or not, so a cold request and a hot one report the same numbers. Measured on 200k × 256d on-disk vectors: the same
HNSW query took 1295 ms with an evicted page cache and 2.7 ms hot, with identical `vector_io_read_bytes`.

So each segment scope also samples the OS counters of the thread that runs it, at start and end, with one
`getrusage(RUSAGE_THREAD)` each (~0.7 µs, only when profiled):

- `major_page_faults` (`ru_majflt`): accesses that waited for a disk read.
- `disk_read_bytes` (`ru_inblock` × 512): bytes the thread caused to be read from the block device, including
  readahead and `MADV_WILLNEED` prefetching. Linux fills `ru_inblock` from task I/O accounting
  (`/proc/thread-self/io` `read_bytes`); a unit test checks they agree.

Both are `null` when unknown: non-Linux, a kernel without task I/O accounting (`disk_read_bytes` only), or a scope
that ended on another thread than it started on. `disk_read_bytes` is the better measure of coldness: a full scan of a
cold 2 MB segment showed 1 major fault and 2 MB read, because sequential readahead and `MADV_WILLNEED` prefetching
bring the rest in without further faults. Record retrieval
after search (payload and vectors for the results, `SegmentsSearcher::retrieve`) runs in one blocking task outside
segment scopes; it is reported as phase `retrieve_records`, the first phase with these counters.

Validation against process-level `/proc/<pid>/stat` and `/proc/<pid>/io` deltas around single requests, cold
(restart + evicted page cache) and hot:

| Request | Cold time | Process: faults / disk | Profile: faults / disk | Hot time | Hot: faults / disk |
|---|---|---|---|---|---|
| HNSW, no filter | 1315 ms | 608 / 80.0 MiB | 608 / 80.0 MiB | 5.4 ms | 0 / 0 |
| HNSW, indexed filter | 224 ms | 967 / 13.2 MiB | 967 / 13.2 MiB | 3.6 ms | 0 / 0 |
| Un-indexed filter (full scan) | 3412 ms | 13141 / 95.8 MiB | 13141 / 95.8 MiB | 45.7 ms | 0 / 0 |
| Scroll + payload | 97 ms | 141 / 0.6 MiB | 36 in segments + 105 in `retrieve_records` / 0.6 MiB | 5.0 ms | 0 / 0 |

When anything was read from disk, a note says so, with the totals over all segments and phases and where it
happened ("Waited for the disk: 608 major page fault(s), 80.0 MiB read from disk, in 11 segment(s). ..."). Bytes read
without a page fault (e.g. `pread`-based storage) count too. On kernels without task I/O accounting (no
`/proc/self/io`), `disk_read_bytes` is `null`, not 0.

### Second review

A second Codex review of the fixes rated 5 findings fixed and 5 partial, and found 9 new issues. The confirmed ones
are fixed: explain rate cost per planned leaf (full cost for `retrieve`), approximate count kept from older peers,
distance counts on peers with the flag disabled, consistent multi-vector and sparse counting units, the disk note
counting all reads, `null` disk bytes without task I/O accounting, tolerant enum conversion, deduplicated notes,
`order_by` explain checking the range index, and less fragile I/O tests. Three findings only applied to mixed versions
with an intermediate, unreleased commit of this branch.

### CodeRabbit review

CodeRabbit's review of the draft PR led to: `segment_search` timed from dispatch; nested filter keys reported with
their full path; fingerprints over the complete request; workload samples for `search` (`core_search`); unknown I/O
kept unknown when merging phase calls; phase I/O counted once inside segment scopes; sparse and custom quantized
scores counted per comparison; TurboQuant scores counted as quantized; no filter resolution in sparse explain; and the
`query_profiling` flag in the OpenAPI schema. Adding the workload endpoints to the OpenAPI spec, JWT access table and
metrics whitelist is left for GA, like `/profiler/slow_requests`.

### Third review

A third Codex review found: O(N²) fingerprinting of distance-matrix batches (now shapes without id sets, and no
`remove_details()` clone); a profiled request permanently enabling distance counting on a peer with the flag off
(now a note instead); a wrong note for older peers' approximate counts; multivector scores counted before a fallible
read; non-deterministic formula fingerprints and formula defaults that failed to serialize (also in the slow log);
"0.0 MiB" instead of an unknown amount without task I/O accounting; nested-phase I/O stripping that could erase a
standalone phase's I/O; and fragile cold-I/O test conditions. All fixed with tests.

### CodeRabbit review, round 2 (checked with Codex)

- Explain no longer resolves filters shared by search groups (`search_shared_filter.rs`). The guard reads the mode from
  the query's handoff: that code runs on a search worker before any profiled scope is entered.
- Search slow-log values keep a bounded, deterministic sample of `has_id` (the smallest 63 ids and a count), built
  without serializing the set; their hash is structural (id sets hash independently of order). Fingerprints keep
  `"?"` for id sets.
- Formula defaults serialize as `{variable, value}`, so the value normalizes as a literal.
- Array deduplication and fingerprint hashes use a canonical string (object keys sorted): `serde_json` keeps key
  order here (`preserve_order`).
- Not changed: fingerprints merge requests that differ only in the number of examples (1 vs 100 recommend
  positives), like filter value lists, by design; per-fingerprint distance counts show the total cost.
- Known gaps: explain with a sparse IDF corpus filter still computes the IDF statistics, as query input (like
  fetching recommend examples by id), and a `has_id` inside `params.idf.corpus` is serialized whole in search log
  values (no production path logs search batches today).

## Access control

- **Per-request profiles need the same access as the request** (read on the collection), not `manage`. A profile only
  describes the collection the caller can already read: segment ids, point counts per segment (already visible via
  collection info), the payload keys *of the caller's own filter*, and timings. Qdrant JWTs scope access to whole
  collections (the payload-scoped claim was removed), so there is no finer tenant boundary that a profile could
  cross. Requiring `manage` would make the feature useless to the application developers who write the queries.
- **Remote shards** only see the mode, and return the profile of their own shard operation for the same collection.
- **`/profiler/workload` needs global `manage`**, like `/profiler/slow_requests`: it aggregates requests of *all*
  collections and tenants on the node. Normalized fingerprints contain no literal values (filter values, ids, shard
  keys, vectors, documents are replaced by `"?"`), but payload key names and collection names are visible.
- Profiles never contain point data, payload values, other collections, or other requests.

## Workload: fingerprints and counters

- Fingerprint = hash(collection, request type, normalized request). Normalization serializes the whole shard-level
  request (`Loggable::to_fingerprint_value`; unlike the slow log's value, arrays are not cut, and batches are normalized
  item by item so memory is bounded by the distinct shapes. Queries and searches use explicit *shapes*
  (`profiling/fingerprint.rs`): vectors are reduced to their dimensions and id sets (`has_id`, resolved points) to
  `"?"` without being serialized, so a distance matrix request (N searches, each with a `has_id` of N ids) costs
  O(N) to fingerprint, not O(N²). Formulas serialize their variables and defaults sorted, so equal formulas
  fingerprint equally (their defaults used to fail to serialize, as a map with non-string keys). The rest
  replaces every number
  and every string under a literal key (`value`, `any`, `except`, `text`, `phrase`, `gt/gte/lt/lte`, `id(s)`,
  `has_id`, `offset`, `nearest`, `positive/negative`, `shard_key`, …) with `"?"`, and squashes arrays to their distinct
  elements, so `should` over 3 cities and a batch of 10 same-shaped queries fingerprint like one (as Postgres 18 squashes
  `IN` lists). Booleans and other strings (payload keys, vector names, `fusion: rrf`) are kept because they change the plan.
- Per fingerprint: calls, errors, total/mean/min/max time, p50/p95 from a fixed log-scale histogram (128 buckets,
  ×1.25, 0.05 ms … minutes, ±12.5%), distance computations (original and quantized), first/last seen.
- Up to 1024 fingerprints per node; when full, the one with the least total time is evicted (`evicted` is reported).
  The stored normalized request is cut to 4 KiB of JSON (the hash covers all of it), so the table stays under ~4 MiB
  whatever the requests contain. Reading the endpoint selects the top entries before cloning them.
- Every request is counted, not only those over 50 ms. Explain-mode and rate-limited requests are not counted.
- Counted at the **shard operation** level, like the slow log: a request touching 3 local shards counts 3 calls.

### Cluster-wide view (proposal, not implemented)

The fixed-bucket histogram makes per-node stats **mergeable exactly**: summing bucket counts across peers gives the
cluster histogram, hence exact cluster p95 (up to the bucket width). Proposal: `GET /profiler/workload?cluster=true`
on any peer fans out an internal gRPC `GetWorkload` to all peers (same channel pool and timeout as
`/profiler/consensus_lag`), merges entries by fingerprint (fingerprints are deterministic across peers built from the
same version) and reports `peers_responded / peers_total`. The slow request log can use the same fan-out. Remote shard
calls are counted on the peer that executed them, so there is no double counting; the coordinator side is visible
as network time in per-request profiles.

## Open questions

1. **Request parsing** (JSON/protobuf decoding, inference) happens before the handler and is not in `phases`;
   `total_time_ms` starts at the handler. Middleware timing could add it.
2. **Filter planning time** is not a separate phase: cardinality estimation runs inside the segment search and is
   included in its `time_ms`. Splitting it needs a timer in `dispatch.rs`, cheap but noisy for tiny segments.
3. `actual_cardinality` for HNSW-filtered searches would need a counter of filter checks in the graph traversal
   (per point); we left it out to keep the hot loop untouched. `payload_io_read_bytes` is the proxy.
4. **Page cache vs. disk — resolved.** See [Real disk I/O](#real-disk-io-page-cache-misses). Remaining gaps: reads
   issued on another thread than the segment scope (rayon fan-out inside a segment, a future io_uring backend that
   completes on its own threads) are not attributed; phases spanning async tasks report no OS counters.
5. OpenAPI: like `/profiler/slow_requests`, the beta endpoints and the `profile` parameter are not in the published
   spec yet. Before GA: add them to `openapi/*.ytt.yaml`, `EXPECTED_NUMBER_OF_APIS`, the JWT `ACTION_ACCESS` table and the
   metrics whitelists.
6. Should `profile` also be allowed on recommend/discover/groups/matrix? They go through the same query path, so it is
   mostly plumbing.
7. Fingerprint cost: one serialization per shard operation, plus a global mutex, while the flag is on. Not measurable
   in our benchmark. If it shows up at high QPS, shard the map by fingerprint hash and/or sample 1/N requests, scaling
   the counters.
8. `profile=analyze` costs a fixed 40–70 µs, more than 5% for sub-millisecond requests. Should empty segments
   (0 points) be omitted from `segments[]` to shrink it? They are noise in the output too.
9. Should the workload stats get their own feature flag, so they can be enabled without per-request profiles
   (or the other way round)?

## Follow-ups enabled by this design

- **Web UI**: render `segments[]` as a heat table (strategy × time) and `notes` as callouts.
- **Persisting profiles**: the slow log could store the `QueryProfile` of the slowest call per fingerprint
  (auto-explain, like `auto_explain.log_min_duration`).
- **Index recommendations**: `unindexed_keys` aggregated over the workload is exactly "keys that would benefit
  from a payload index, weighted by time".
