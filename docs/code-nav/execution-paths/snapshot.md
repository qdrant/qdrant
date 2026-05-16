# Snapshots: execution path

## Overview

Snapshots are **point-in-time backups** of vector data and supporting metadata. A **collection snapshot** is a single archive (conventionally named `*.snapshot`) built as a **tar** stream: per-shard subtrees (segments, optional WAL, replica/shard config, clock data), plus **collection-level** files (storage version, collection config, shard key mapping, payload index schema). A **full storage snapshot** first creates one collection snapshot per accessible collection, writes a JSON mapping file, then archives those artifacts plus `config.json` into one top-level tar.

## Snapshot Creation Path

End-to-end flow for a **collection snapshot** (see `Collection::create_snapshot` in `lib/collection/src/collection/snapshots.rs`):

1. **API** — REST handlers in `src/actix/api/snapshot_api.rs` delegate to storage helpers (`do_create_full_snapshot`, collection snapshot routes, shard snapshot routes) and `TableOfContent` / `Dispatcher` as appropriate.
2. **TableOfContent** — For a named collection, `toc.create_snapshot` resolves the `Collection` and calls `Collection::create_snapshot` (storage wiring in `lib/storage/src/content_manager/snapshots/mod.rs` and TOC methods).
3. **Collection** — Under a global temp directory: create a temp staging dir and a temp archive file; for each `(shard_id, replica_set)` in the shard holder, schedule `ShardReplicaSet::create_snapshot` into a tar subtree rooted at the shard path (`shard_path`). Non-listener nodes typically pass `save_wal = true` so the WAL is included; listeners may snapshot without WAL (segments still consistent via other mechanisms in that mode).
4. **Shard / replica set** — `ShardReplicaSet::create_snapshot` (`lib/collection/src/shards/replica_set/snapshots.rs`) obtains a snapshot future from the **local** shard’s `get_snapshot_creator`, then appends **replica set state** and **shard config** (`ShardConfig::new_replica_set`) into the same tar prefix.
5. **Local shard → segments** — `LocalShard::get_snapshot_creator` (`lib/collection/src/shards/local_shard/snapshot.rs`) runs blocking work: `snapshot_all_segments` flushes segments and calls each segment’s `take_snapshot` (see `Segment` in `lib/segment/src/segment/snapshot.rs`), optionally embedding `segment_manifest.json` under each segment directory for **partial** snapshot formats. It archives **clock map data** via `LocalShardClocks::archive_data`, then either copies the real WAL + `applied_seq` or generates a compatible **empty WAL** when `save_wal` is false.
6. **Finalize collection tar** — The collection layer appends `VERSION_FILE`, `COLLECTION_CONFIG_FILE`, shard key mapping (`save_key_mapping_to_tar`), and payload index config (`PAYLOAD_INDEX_CONFIG_FILE`), then finishes the tar and moves it to the collection’s snapshots directory via `SnapshotStorageManager::store_file`.

**Full snapshot** (`_do_create_full_snapshot` in `lib/storage/src/content_manager/snapshots/mod.rs`): iterate collections (subject to multipass), create each collection snapshot, write `SnapshotConfig` (collection → snapshot file names + alias map) to a temp JSON file, copy per-collection snapshot files to temp paths, then `TarBuilder` packs those blobs plus `config.json` into the final full snapshot archive.

## Snapshot Contents

| Layer | Contents (typical) |
|--------|---------------------|
| **Collection archive root** | Storage version file, `config.json` (collection config), shard key mapping file when used, `payload_index.json` (payload index schema). |
| **Per-shard prefix** | Replica state file, shard config marking replica-set layout; under local replica path: `segments/` with per-segment dirs (vector/payload/index files; optional `segment_manifest.json` for partial snapshots), **WAL** subtree (`WAL_PATH`) and **`applied_seq`** when WAL is saved, or empty WAL + clocks when not. |
| **Segment** | `Segment::take_snapshot` writes segment files into the tar (and optional manifest JSON under `files/segment_manifest.json` when merging with a baseline manifest). |
| **Clocks** | `LocalShardClocks::archive_data` includes clock-related persisted data alongside the shard snapshot for consistency with replicated updates. |

`SnapshotManifest` / `RecoveryType` in `lib/shard/src/snapshots/snapshot_manifest.rs` classify whether segment manifests are expected (**full** vs **partial** recovery).

## Snapshot Recovery

**Collection from uploaded / URL snapshot** — `do_recover_from_snapshot` (`lib/storage/src/content_manager/snapshots/recover.rs`):

- **Download** — `download_snapshot` resolves `SnapshotRecover.location` into `SnapshotData` (`Packed` tar path or `Unpacked` dir) under temp/storage paths; optional SHA256 verification.
- **Extract / restore tree** — `spawn_blocking` calls `Collection::restore_snapshot` (`lib/collection/src/collection/snapshots.rs`): unpack tar with `tar_unpack_file` (or move unpacked tree), load and validate `CollectionConfigInternal`, enumerate shard ids (from config or custom shard key mapping file), and for each shard load `ShardConfig` and dispatch to `LocalShard::restore_snapshot`, `RemoteShard::restore_snapshot`, or `ShardReplicaSet::restore_snapshot` depending on `ShardType`.
- **Reconcile with running cluster** — If the collection already exists, config compatibility checks run; replicas may be moved to a recovery state and data merged from the temp tree into live shard paths (`recover_local_shard_from`, `activate_shard`, consensus proposals when distributed). If the collection does not exist, recovery submits `CollectionMetaOperations::CreateCollection` and payload-index operations through the **dispatcher** so consensus and disk layout stay aligned.

**Shard-level helpers** — `LocalShard::restore_snapshot` delegates to `SnapshotUtils::restore_unpacked_snapshot` (`lib/collection/src/shards/local_shard/snapshot.rs`). Partial vs full shard recovery paths use `SnapshotManifest::load_from_snapshot` validation rules.

## Cross-References

- **HTTP API** — `src/actix/api/snapshot_api.rs` (create/list/delete/recover, full and per-collection/shard).
- **Storage orchestration** — `lib/storage/src/content_manager/snapshots/mod.rs` (full snapshot, deletes, lists), `lib/storage/src/content_manager/snapshots/recover.rs` (recover pipeline), `lib/storage/src/content_manager/snapshots/download.rs` (fetch).
- **Collection** — `lib/collection/src/collection/snapshots.rs` (`create_snapshot`, `restore_snapshot`, shard snapshot entry points).
- **Replica set** — `lib/collection/src/shards/replica_set/snapshots.rs` (`create_snapshot`, `restore_snapshot`, partial snapshot locking).
- **Local shard** — `lib/collection/src/shards/local_shard/snapshot.rs` (segments, WAL, applied seq, clocks).
- **Segment** — `lib/segment/src/segment/snapshot.rs` (`take_snapshot`, manifest file name).
- **Shard snapshot types** — `lib/shard/src/snapshots/snapshot_data.rs`, `snapshot_manifest.rs`, `snapshot_utils.rs` (module root `lib/shard/src/snapshots/mod.rs`).
