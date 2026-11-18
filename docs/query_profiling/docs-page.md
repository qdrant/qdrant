---
title: Query profiling
weight: 200
---

# Query profiling (beta)

_Available as of Qdrant v1.21 (beta)._ <!-- version TBD -->

When a query is slow, Qdrant can tell you why. Add `profile=analyze` to a search, query, scroll, count or facet
request, and the response includes a breakdown of where the time went: which shards and segments were searched,
which strategy each segment chose and why, how many vectors were scored, and which filter conditions had no payload
index. `profile=explain` shows the plan without running the search.

This is similar to `EXPLAIN ANALYZE` in PostgreSQL and MySQL, or `db._profileQuery` in ArangoDB.

Query profiling is in beta. Enable it with the `query_profiling` feature flag:

```yaml
# config.yaml
feature_flags:
  query_profiling: true
```

or `QDRANT__FEATURE_FLAGS__QUERY_PROFILING=true`. Requests without the `profile` parameter are not affected.

## Profiling a request

```http
POST /collections/{collection_name}/points/query?profile=analyze
{
  "query": [0.2, 0.1, 0.9, 0.7],
  "filter": { "must": [{ "key": "color", "match": { "value": "red" } }] },
  "limit": 10
}
```

The `profile` parameter is supported by:

| Endpoint | `analyze` | `explain` |
|---|---|---|
| Query, query batch (`/points/query`, `/points/query/batch`) | ✓ | ✓ |
| Search, search batch (`/points/search`, `/points/search/batch`) | ✓ | ✓ |
| Scroll (`/points/scroll`) | ✓ | ✓ |
| Count (`/points/count`) | ✓ | ✓ |
| Facet (`/facet`) | ✓ | — (a facet always reads one payload index; there is no plan to show) |

In gRPC, set `profile: ProfileAnalyze` or `profile: ProfileExplain` on the request; the response carries the same
`QueryProfile` message.

- **`analyze`** runs the request as usual and adds `profile` to the response. The overhead is small but not zero
  (see [Overhead](#overhead)); don't enable it for every request in production.
- **`explain`** runs the planning steps (cardinality estimation and, where needed, sampling, which are real), but
  scores no vectors and returns an empty result. Stages that work on prefetch results (rescoring, fusion,
  grouping) have nothing to plan in explain mode.

Profiles need the same access as the request itself. A profile only describes the collection you queried.

## Reading a profile

```
profile
├── total_time_ms, phases[]   time on the node that received the request: shards, merge, collection_rescore, …
├── shards[]                  one entry per shard: local or remote, peer_id, time_ms, network_time_ms
│   ├── phases[]              prefetch, segment_search, rescore, fusion, merge, payload_retrieval
│   └── segments[]            one entry per segment: strategy, strategy_reason, filter, counters
└── notes[]                   findings, e.g. a missing payload index
```

Start with **`notes`**, which summarize the findings. Then look at the **segments** that take the most `time_ms`:

| Field | Meaning |
|---|---|
| `indexed` | The segment has an HNSW index for this vector. `false` means brute force, e.g. while the optimizer is indexing. |
| `strategy` | `hnsw`, `hnsw_filtered`, `acorn`: graph search. `payload_index_first`: points matching the filter are taken from payload indexes and scored directly. `full_scan`: every point is checked. `exact`: brute force because `exact: true` was set. `sparse_index`. For scroll/count/facet: `indexed_read` or `full_scan_read`. |
| `strategy_reason` | Why: `not_indexed`, `small_segment`, `cardinality_below_threshold`, `cardinality_above_threshold`, `sampled_cardinality`, `exact_requested`, `unfiltered`, … |
| `full_scan_threshold` | The threshold, in points, that the filter cardinality was compared with. |
| `filter.estimated_cardinality` | `min` / `exp` / `max` points expected to match the filter in this segment. |
| `filter.actual_cardinality` | The exact number of matches, when the request computed it anyway (exact count, full scan). |
| `filter.indexed_keys` / `unindexed_keys` | Payload keys that used a payload index / that are checked by reading each candidate's payload. |
| `filter.full_payload_scan` | No payload index narrowed the candidates down: every point of the segment was a candidate. |
| `counters.vector_scores` / `quantized_vector_scores` | Distance computations: the number of "points visited". |
| `counters.*_io_read_bytes` | Bytes read from disk (vectors, payload, payload indexes, object storage). Reads from RAM storage are not counted. |

In a distributed cluster, a remote shard's `time_ms` is measured on the peer that holds it, and `network_time_ms` is
the rest of the round trip (network, serialization, queueing).

## Example: a filter that misses a payload index

A `products` collection with 50 000 points has a payload index on `city`, but not on `color`. This query is slow:

```http
POST /collections/products/points/query?profile=analyze
{ "query": [...], "limit": 10, "filter": { "must": [{ "key": "color", "match": { "value": "red" } }] } }
```

The profile (trimmed):

```json
{
  "time": 1.221,
  "profile": {
    "total_time_ms": 1222.2,
    "shards": [{
      "phases": [{ "name": "segment_search", "time_ms": 1192.0, "calls": 1 }, ...],
      "segments": [
        {
          "points": 40000, "indexed": true,
          "strategy": "full_scan", "strategy_reason": "sampled_cardinality", "full_scan_threshold": 20000,
          "filter": {
            "estimated_cardinality": { "min": 0, "exp": 20000, "max": 40000 },
            "indexed_keys": [], "unindexed_keys": ["color"], "full_payload_scan": true
          },
          "time_ms": 1185.5,
          "counters": { "vector_scores": 13334, "payload_io_read_bytes": 1399894, ... }
        },
        {
          "points": 10000, "indexed": false, "appendable": true,
          "strategy": "full_scan", "strategy_reason": "not_indexed",
          "filter": { "unindexed_keys": ["color"], "full_payload_scan": true, "actual_cardinality": 3333, ... },
          "time_ms": 1004.6
        }
      ]
    }],
    "notes": [
      "Filter conditions on `color` have no payload index: they are checked by reading the payload of each candidate point. Consider creating a payload index.",
      "2 segment search(es) found no payload index to narrow down the filter and considered every point of the segment."
    ]
  }
}
```

What it says:

1. Without an index on `color`, Qdrant can only estimate that between 0 and 40 000 points match (`min: 0, max: 40000`).
   It sampled points to decide, and chose a **full scan**: it read the payload of all 40 000 points
   (`payload_io_read_bytes`: 1.4 MB) to find the 13 334 red ones.
2. The time goes into payload reads, not vector scoring: only 13 334 vectors were scored.

`profile=explain` shows the same plan in a few milliseconds, without running the scan. Create the index:

```http
PUT /collections/products/index
{ "field_name": "color", "field_schema": "keyword" }
```

and profile again:

```json
{
  "time": 0.0021,
  "profile": {
    "shards": [{ "segments": [{
      "points": 40000,
      "strategy": "payload_index_first", "strategy_reason": "cardinality_below_threshold",
      "full_scan_threshold": 20000,
      "filter": {
        "estimated_cardinality": { "min": 13334, "exp": 13334, "max": 13334 },
        "indexed_keys": ["color"], "unindexed_keys": [], "full_payload_scan": false
      },
      "time_ms": 1.4,
      "counters": { "vector_scores": 13334, "payload_io_read_bytes": 0 }
    }, ...] }],
    "notes": []
  }
}
```

The estimate is now exact, the matching points come straight from the index, no payload is read, and the request
takes 2 ms instead of 1.2 s.

The same note appears when a `should` branch is not indexed: one un-indexed condition in a `should` makes the whole
branch impossible to resolve from indexes, which `full_payload_scan: true` shows.

## Example: a query that hits unoptimized segments during indexing

An `articles` collection with 50 000 points answers an unfiltered query in about 8 ms. After 50 000 more points
are uploaded, the collection is `yellow` while the optimizer builds the index:

```http
POST /collections/articles/points/query?profile=analyze
{ "query": [...], "limit": 10 }
```

```json
{
  "profile": {
    "shards": [{
      "phases": [{ "name": "segment_search", "time_ms": 0.85, "calls": 1 }, ...],
      "segments": [
        { "points": 40000, "indexed": true,  "strategy": "hnsw",      "strategy_reason": "unfiltered",  "time_ms": 0.51, "counters": { "vector_scores": 2779 } },
        { "points": 20000, "indexed": false, "strategy": "full_scan", "strategy_reason": "not_indexed", "time_ms": 0.73, "counters": { "vector_scores": 20000 } },
        { "points": 20000, "indexed": false, "strategy": "full_scan", "strategy_reason": "not_indexed", "time_ms": 0.71, "counters": { "vector_scores": 20000 } },
        { "points": 20000, "indexed": false, "appendable": true, "strategy": "full_scan", "strategy_reason": "not_indexed", "time_ms": 1.54, "counters": { "vector_scores": 20000 } }
      ]
    }],
    "notes": [
      "3 segment(s) with 60000 points have no vector index and are searched by brute force (85% of segment search time). This is expected for segments smaller than `indexing_threshold` and while the optimizer is indexing; check `optimizer_status` and `indexing_threshold`."
    ]
  }
}
```

What it says:

1. The indexed segment scores 2 779 vectors for 40 000 points: that is HNSW at work.
2. The three new segments are not indexed yet, so every one of their 60 000 vectors is scored
   (`vector_scores` = `points`). They take 85% of the search time.
3. This resolves itself when indexing finishes. If it persists, check the collection's `optimizer_status`, and whether
   `indexing_threshold` is higher than the segment size.

Even on a `green` collection you may see one small appendable segment with `indexed: false`: segments smaller than
`indexing_threshold` are not indexed, because brute force is fast enough for them.

## Workload statistics

A single profile explains one request. To find *which* requests to profile, use the workload statistics, similar to
`pg_stat_statements`:

```http
GET /profiler/workload?sort=total_time&limit=10
```

```json
{
  "version": 1, "enabled": true, "since": "2026-11-18T14:48:43.683Z", "evicted": 0,
  "fingerprints": [
    {
      "fingerprint": "79a1115a93a16f26",
      "collection_name": "bench",
      "request_name": "query",
      "normalized_request": [{
        "prefetches": [],
        "query": { "Vector": { "Nearest": { "query": { "Dense": ["?"] }, "using": "" } } },
        "filter": { "must": [{ "key": "color", "match": { "value": "?" } }] },
        "limit": "?", "offset": "?", "with_vector": false, "with_payload": false
      }],
      "calls": 300, "errors": 0,
      "total_time_ms": 8171.4, "mean_time_ms": 27.24, "min_time_ms": 20.61, "max_time_ms": 56.38,
      "p50_time_ms": 25.85, "p95_time_ms": 50.49,
      "vector_scores": 10000000, "quantized_vector_scores": 0,
      "first_seen": "2026-11-18T14:48:43.683Z", "last_seen": "2026-11-18T14:48:52.541Z"
    },
    {
      "fingerprint": "d54a1d7ae93b5db2",
      "collection_name": "bench",
      "request_name": "query",
      "normalized_request": [{ "...": "...", "filter": { "must": [{ "key": "city", "match": { "value": "?" } }] } }],
      "calls": 300, "errors": 0,
      "total_time_ms": 121.5, "mean_time_ms": 0.40, "min_time_ms": 0.23, "max_time_ms": 0.86,
      "p50_time_ms": 0.47, "p95_time_ms": 0.73,
      "vector_scores": 300000, "quantized_vector_scores": 0,
      "first_seen": "...", "last_seen": "..."
    }
  ]
}
```

The two fingerprints are the same query with a filter on different keys. The first one (`color`, no payload index)
takes 67 times longer and scores 33 times more vectors per call: profile one of those requests to see why.
The 300 calls of the second one used 100 different `city` values and limits of 5, 10 and 20; they share one
fingerprint.

Requests of the same shape share a fingerprint: vectors, filter values, ids, limits and shard keys are replaced by
`"?"`, and lists are reduced to their distinct shapes, so `match: {value: "red"}` and `match: {value: "blue"}` count
together. Payload keys, vector names and options like `exact` or `fusion` are kept.

- Statistics are per node and kept in memory, for up to 1024 fingerprints. They are counted per shard operation, so a
  request that touches 3 shards on a node counts 3 calls on that node.
- `p50_time_ms` and `p95_time_ms` are accurate to ±12.5%.
- `sort`: `total_time` (default), `mean_time`, `p95_time`, `calls`, `vector_scores`. Filter with `collection=<name>`.
- `DELETE /profiler/workload` resets the statistics.
- Like `/profiler/slow_requests`, this endpoint needs `manage` access: it covers all collections on the node.

Statistics are collected only when the `query_profiling` feature flag is enabled.

## Overhead

- With the feature flag **disabled**, or enabled but without `profile` in the request, there is no measurable
  overhead.
- `profile=analyze` adds a fixed 40–70 µs per request (building and returning the profile): under 5% for requests
  that take more than ~1.5 ms, up to 8% for sub-millisecond requests.

## Limitations

- Request parsing and inference time are not part of `phases`; `total_time_ms` starts when the request is handled.
- Filter planning time is included in each segment's `time_ms`, not reported separately.
- `actual_cardinality` is only reported when the request computes it anyway (exact count, full scan).
- Reads from the OS page cache and from disk are not distinguished.
- Profiles are not stored: each one is returned with its response only.
