# Index-backed payloads for internal scrolls

An internal scroll can avoid reading and deserializing payload storage when its
consumer only needs the values of explicitly selected indexed fields. This draft
implements that opt-in without changing public REST or gRPC requests.

## Internal contract

Set `ScrollRequestInternal::prefer_payload_index` to `true` and use an explicit
field list or include selector:

```rust
ScrollRequestInternal {
    with_payload: Some(WithPayloadInterface::Fields(vec!["city".parse()?])),
    prefer_payload_index: true,
    ..Default::default()
}
```

The default is `false`: retrieve the original payload and apply its selector.
With the hint enabled, eligible top-level fields are returned as arrays of their
indexed values. For example, `{"city": "Berlin"}` becomes
`{"city": ["Berlin"]}`. Values not represented by the index are omitted, and a
field with no indexed values is omitted. The caller must accept this loss of
shape, ordering, multiplicity, and original representation.

The hint permits exact fallback. If any selected field has no usable index, or
any path is nested, the whole projection uses the existing batched payload read
for that segment. `with_payload: true` and exclude selectors also retain exact
retrieval. `with_payload: false` still skips payload materialization. An empty
include list returns empty payloads without storage reads.

Restricting the initial implementation to top-level fields avoids synthesizing
an ambiguous JSON hierarchy. Nested array indexes lose the association between
values in different objects; returning them in an invented object layout would
create false correlations. Literal top-level keys containing dots remain
supported through quoted JSON paths.

| Index | Retrieved values and relevant differences |
| --- | --- |
| Keyword | Strings; original scalar/array shape is lost |
| Integer / integer lookup | Accepted integer values only |
| Float | Numbers; original numeric representation is not preserved |
| Boolean | Distinct values in bitmap order |
| UUID | Canonical UUID strings |
| Datetime | Normalized datetime strings |
| Geo | Coordinates without surrounding object layout |
| Full text / null | Exact payload fallback; no original-value retriever |

Index presence does not enforce a field's type. Mixed-type arrays, nulls, and
empty arrays therefore cannot be interpreted as their exact stored JSON through
this mode. Consumers that copy documents or depend on the original shape must
keep the default.

## Read path

Currently, `SegmentReadView::requested_payloads` reads payload storage whenever
payload is enabled, and only then applies the include/exclude selector. Selecting
one indexed field still reads and parses the stored payload.

The new path prepares index value retrievers once per segment and batch, then
uses the point offsets already resolved by normal record retrieval. It preserves
point selection, sorting, pagination, cross-segment version resolution, proxy
segment handling, and deferred-point visibility. Filtering may independently
read payload storage; this optimization concerns result materialization.

Formula rescoring already has index value retrievers, but their per-point return
type is infallible. Some underlying `get_values` implementations convert I/O
errors to missing values. The new projection instead visits values through the
fallible index checkers, with a predicate that always returns false to visit all
values. It propagates errors instead of returning an incomplete payload. Boolean
retriever construction also propagates bitmap loading errors.

Writable and read-only segments share the projection implementation through
`StructPayloadIndexReadView`. `LoadProfile` retains the configured placement of
selected field indexes and conservatively retains payload-storage placement
because exact fallback is allowed. An index can itself be cold or on disk; the
optimization does not guarantee zero I/O.

Relevant code:

- [Scroll request and load profile](../../lib/shard/src/scroll.rs)
- [Local scroll](../../lib/collection/src/shards/local_shard/scroll.rs)
- [Version-aware retrieval](../../lib/shard/src/retrieve/retrieve_blocking.rs)
- [Payload materialization](../../lib/segment/src/segment/read_view/search.rs)
- [Projection planning](../../lib/segment/src/index/struct_payload_index/read_view/payload_index_read.rs)
- [Fallible value collection](../../lib/segment/src/index/field_index/payload_value_retriever.rs)

## Public API and distributed reads

Despite its name, `ScrollRequestInternal` is flattened into the public REST
request. Its hint is marked `#[serde(skip)]` and `#[schemars(skip)]`, as is the
hint in the lower-level `WithPayload` type. Public JSON cannot enable it, and
public gRPC requests explicitly initialize it to false. `WithPayloadInterface`,
public `ScrollPoints`, and public `WithPayloadSelector` remain unchanged.

Only the outer internal `ScrollPointsInternal` protobuf message gains a boolean
field. The remote shard sends it and the internal service restores it. An old
sender omits the field and gets exact behavior. An old receiver ignores it and
returns exact payloads, which is allowed by the hint's contract. Callers must
extract field values in a way that accepts both scalar and array forms.

Replica resolution compares payloads. Two equal stored documents could otherwise
produce unequal responses if one replica uses indexes and the other falls back
to storage. Requests with a consistency setting other than the default factor
of one therefore use exact retrieval before dispatch. Payload equality and
public consistency behavior are unchanged.

The Edge read path also carries the internal hint; public Edge request
conversion initializes it to false. Existing transfer scrolls remain exact.
Cleanup and filter-to-ID resolution already request no payload. No existing
consumer is switched on automatically.

Group candidate collection is a possible later consumer: it requests the group
field and hydrates final results afterward. It uses universal queries, however,
and would need separate policy propagation plus an audit of mixed-type and
normalized group-key semantics. It is outside this scroll-only change.

## Validation and remaining scope

Targeted tests cover eligible value types and normalization, missing/null/empty
fields, exact fallback for nested/unindexed/full-text fields, public serialization
and schema exclusion, read-only segments, ID/ordered scrolls, and consistency
fallback. The index projection is also exercised while payload storage is
exclusively borrowed, so an attempted storage access would fail. Hardware counters
check that record retrieval performs no payload-storage reads on the fast path.
A corrupted on-disk index tests error propagation.

Nested projections and a strict index-only guarantee are intentionally outside
the initial contract. A strict guarantee would require rejecting unsupported
projections and negotiating support with old peers rather than accepting fallback.
No latency speedup is claimed; a performance evaluation should compare large cold
payloads against small indexed projections, with both cold and warm indexes, and
record payload reads, index reads, CPU time, and latency separately.
