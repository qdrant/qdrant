# Comparing stored fingerprints with block hashes

`POST /collections/{collection_name}/points/block-hashes` performs a read-only,
on-demand audit. It reads IDs and one selected payload field, without vectors.
There are no maintained hashes, repair actions, point-hash responses, or snapshot
sessions. Use ordinary collection read authorization and optional `?timeout=N`
(seconds). The timeout covers the **whole audit**, including hashing, rather than
restarting for every internal page. Standard read consistency/routing and strict
mode checks apply; no additional consistency parameter is required.

```json
{"payload_key":"sync.fingerprint","block_count":16,"filter":{"must":[{"key":"tenant","match":{"value":"tenant-a"}}]}}
```

`payload_key` and `block_count` are required; `filter` is optional. `block_count`
is an integer in `1..=65536`. The cap bounds accumulator memory and response size;
strict mode's query limit may impose a smaller bound. Strict mode can also forbid
exact scans, unindexed filtering, or excessive timeouts. Unknown request fields
are rejected. All blocks are returned in increasing `block_id` order, including
empty blocks:

```json
{"result":{"blocks":[{"block_id":0,"point_count":0,"hash":"051fd05467e63a94ef3a80b1a1b31f2a28c475473d22e3a172f77dc22e25c80e"}]},"status":"ok"}
```

The usual response envelope also includes timing and, when enabled, hardware usage.

## Content contract v1

The following byte contract is fixed; there is no algorithm selector. All lengths
are byte lengths, all integers in content framing are unsigned **big endian**, and
all digests are SHA-256. `||` means byte concatenation, not text concatenation.

1. Apply the request filter and resolve logical points using Qdrant's ordinary
   ID-ordered scroll semantics, including segment deduplication and latest-version
   payload retrieval. The source must also have unique logical IDs. A live audit
   sees the same visibility rules as scroll, including deferred points.
2. Compute `block_id = slice_point_id_hash(id) % block_count`. This is exactly
   `Slice::check`: SipHash-2-4 with an all-zero key, over 8 **little endian** bytes
   for numeric IDs, or the 16 RFC 4122 UUID bytes. This existing membership contract
   is independent of the content framing below.
3. Encode an ID as `0x00 || u64be(id)` for numeric IDs, or
   `0x01 || uuid_rfc4122_bytes` for UUIDs. UUID spelling/case does not affect the
   digest. Source numeric IDs must be unsigned 64-bit integers, not floating-point
   approximations or decimal strings. JavaScript clients must preserve integers
   above `2**53 - 1` when parsing JSON (for example with a lossless JSON parser).
4. Select a **single string** at the object path `payload_key`. Encode its exact
   UTF-8 bytes without Unicode normalization, trimming, case folding, or parsing a
   presumed hexadecimal fingerprint. Empty strings and embedded NULs are valid.
   Canonically equivalent Unicode strings with different bytes hash differently.
5. Compute each record digest:

   ```text
   R = SHA256(ASCII("qdrant:point-fingerprint:v1") || 0x00
              || encoded_id || u64be(utf8_byte_length) || utf8_bytes)
   ```

6. Within each block, order records by ID: numeric IDs first in ascending unsigned
   numeric order, then UUIDs in lexicographic RFC 4122 byte order. Let `R1..Rn` be
   the raw 32-byte record digests in that order. Compute:

   ```text
   B = SHA256(ASCII("qdrant:block-hashes:v1") || 0x00
              || R1 || ... || Rn || u64be(n))
   ```

   Return `n` as `point_count` and `B` as 64 lowercase hexadecimal characters.
   Fixed-size record digests and the final count provide unambiguous framing.
   Payload path, filter, block number, and block count are not hashed: identical
   ordered records produce identical hashes, including during refinement.

An empty block hashes the block domain followed by eight zero bytes. All empty
blocks therefore have the same digest. Hashes include identity, so exchanging
two fingerprints between different IDs changes the affected block hashes.

### Payload paths and errors

V1 supports Qdrant's object-path syntax, including quoted object keys, such as
`sync.fingerprint` or `"sync.key"."finger print"`. Every intermediate value must
be an object. Array indices (`[0]`) and wildcards (`[]`) are rejected even if they
would happen to select one string. Quoted keys follow Qdrant JsonPath syntax:
quotes and backslash escapes inside a quoted key are not supported.

A missing field, missing intermediate object, absent payload, null, number,
boolean, array (even a one-element string array), or object instead of a string
fails the **entire request** with a normal bad-request response identifying the
point and path. No participating point is silently skipped. Values on points
excluded by the filter do not matter. Invalid paths and block counts fail request
validation. Shard errors, cancellation, and timeouts cannot produce a successful
partial summary.

## Reference client and test vectors

[tools/block_hashes.py](../tools/block_hashes.py) is a dependency-free Python
reference, including slice membership, object-path selection, canonical encoding,
and aggregation. It accepts a JSON array of `{ "id": ..., "payload": ... }`
records already scoped to the same source filter. It rejects duplicate source IDs.
It sorts in memory; clients handling large datasets can instead externally sort
or merge ID-ordered streams and maintain one SHA-256 state per block.

```sh
python3 tools/block_hashes.py --self-test
python3 tools/block_hashes.py source.json --payload-key sync.fingerprint --block-count 16
python3 tools/block_hashes.py source.json --payload-key sync.fingerprint --block-count 64 --slice 16 3
```

[block-hashes-v1.json](block-hashes-v1.json) publishes ID encodings, UTF-8 bytes,
slice hashes (decimal strings to avoid JSON precision loss), record digests, and
complete block responses. Rust and Python tests consume the same fixtures. Cases
cover numeric extremes, UUID extremes, UTF-8, NUL, empty strings, quoted paths,
unordered input, empty datasets, and modulo refinement. The reference does not
implement Qdrant's general filter language; evaluate equivalent predicates in the
source system before hashing.

## Refinement and retrieval

If block 3 of 16 differs, restrict to that slice and increase resolution to a
**multiple** of 16. Retain every original scope predicate:

```json
{"payload_key":"sync.fingerprint","block_count":64,"filter":{"must":[{"key":"tenant","match":{"value":"tenant-a"}},{"slice":{"total":16,"index":3}}]}}
```

Only blocks 3, 19, 35, and 51 can be nonempty. All 64 blocks are returned. A
`block_count` of 4 does not create four children of the old block. Non-multiple
resolutions are valid partitions but do not form a hierarchy.

Scroll each remaining mismatched slice to compare records by ID:

```http
POST /collections/products/points/scroll
```

```json
{"filter":{"must":[{"key":"tenant","match":{"value":"tenant-a"}},{"slice":{"total":64,"index":19}}]},"with_payload":["sync.fingerprint"],"with_vector":false,"limit":1000}
```

Continue with the returned `next_page_offset`. Request extra payload or vectors
only if needed for repair, using existing read/write APIs. There is no separate
point-hashing endpoint.

## Live consistency and cost

This is a live audit, including within one request. Concurrent inserts, updates,
deletes, replica divergence, or shard movement may affect results and pagination.
It does not lock a dataset for the duration of a comparison. Repeat comparisons
and revalidate before destructive repairs. A destination-only ID while a sync is
running is insufficient evidence for safe deletion. Custom shard keys can contain
overlapping ID spaces; as with collection-wide scroll, clients should use globally
unique IDs for this collection-wide audit.

The coordinator consumes globally ordered scroll pages (1024 points per page),
updates all blocks in the same scan, and finalizes only after every page succeeds.
It does **not** concatenate segment/shard hashes. This deliberately preserves the
ordered contract across local and remote shards without a new internal RPC. Only
the requested payload projection and IDs cross the internal RPC boundary; vectors
are never requested. Memory is bounded by the accumulators and a page per shard,
subject to stored payload sizes. Hashing runs on the search blocking runtime and
checks cancellation between records and large string chunks.

Client reconciliation traffic falls from all IDs/fingerprints to block summaries
plus records in mismatched slices. Internal cluster traffic still includes scanned
IDs/fingerprints. Global paginated merging can also fetch ahead on each shard,
so it may transmit/read some records more than once. Payload storage may read more
than the selected field to serve a projection. Without maintained summaries, the
initial audit must scan the participating data. Slice filters and repeated
refinement may incur substantial further scans; they do not guarantee indexed
slice access or reduced server I/O. Benchmark before choosing block counts or
refinement depth.

## Benchmarking

Use [tools/benchmark_block_hashes.py](../tools/benchmark_block_hashes.py) against a
disposable or otherwise appropriate quiescent collection. The benchmark is
read-only, measures a scroll baseline, aggregate audit, and an end-to-end
reconciliation with simulated source fingerprint differences. It also compares
concurrent query latency with a no-audit baseline. Supply local server PIDs to
measure CPU and Linux process I/O, and a representative query JSON file:

```sh
python3 tools/benchmark_block_hashes.py --url http://localhost:6333 \
  --collection products --payload-key sync.fingerprint --block-count 16 \
  --mismatches 1 --refine-factor 4 --query query.json --pid 12345 --seconds 5
```

Run repeated warm/cold-cache trials, across shard counts, payload sizes, filter
selectivities and mismatch rates. Process counters include concurrent query and
background work. Report disk `read_bytes` separately from logical payload I/O;
a warm-cache zero is not evidence that no payload was scanned. JSON byte totals
exclude HTTP/TLS headers and internal cluster traffic. Do not infer production
throughput from small debug-build runs.

### Local measurement (2026-09-05)

[Raw results and dataset configuration](block-hashes-benchmark.json) record three
warm-cache trials of 20,000 numeric IDs, 64-byte fingerprint strings, 256 bytes of
unrelated payload per point, 32-dimensional vectors, three local shards, on-disk
payload, and vector indexing disabled. This used the dev build, a continuous audit
loop, and one query worker with a 20 ms pause between completed queries. Values
below are medians across trials:

| Workload | Seconds/audit | JSON bytes/audit | Payload read bytes/audit | Query p95 (ms) | CPU seconds/wall second |
| --- | ---: | ---: | ---: | ---: | ---: |
| Query only | — | — | — | 6.68 | 0.51 |
| Scroll all fingerprints | 1.287 | 2,296,187 | 7,007,676 | 10.08 | 1.30 |
| 16 block hashes | 0.440 | 2,042 | 7,007,676 | 12.87 | 2.46 |
| Hash, refine to 64, scroll one mismatch | 0.551 | 44,580 | 7,235,199 | 12.66 | 2.18 |

The initial summary reduced client JSON traffic by 99.91%; the complete simulated
one-change reconciliation reduced it by 98.06%. Both full scans reported the same
payload I/O, and all audit modes reported zero vector I/O. Physical disk reads
were zero in these warm-cache trials. Continuous hashing completed more audits per
second and used more CPU per wall second, with higher query p95 than the scroll
load. CPU includes query/background activity; these are load-impact measurements,
not isolated hash-function costs. Cold-cache and production-scale results remain
deployment-specific and are not established by this sample.
