//! JSON Lines trace writer for the soak run, designed for **post-mortem debugging
//! and run-to-run diffing** — both by humans and by AI agents reading the file.
//!
//! # Where
//!
//! `<storage_path>/trace.log`. Overwritten on every run (`File::create` truncates).
//!
//! # Format
//!
//! One JSON object per line. Compact (`serde_json::to_string`), so a plain
//! `grep '"id":157'` or `grep '"ids":\[.*157' trace.log` reconstructs every op
//! that touched a misbehaving point.
//!
//! The writer flushes after every line, so a panic preserves the trace up to
//! and including the failing op.
//!
//! # Event kinds
//!
//! Every event has a `kind` field. Lifecycle events also have an `op` field
//! (the iteration counter from `run`'s main loop, NOT a globally unique op_num);
//! op events have `op` plus per-op fields.
//!
//! | `kind`         | When                                            | Extra fields |
//! |----------------|-------------------------------------------------|---|
//! | `Header`       | First line — run configuration                  | `seed`, `op_num`, `shard_count`, `id_pool`, `disable_optimizer`, `max_segment_size_kb`, `indexing_threshold_kb`, `flush_interval_sec`, `restart_probability`, `swarm_interval`, `enable_force_off`, `duration_sec` (null unless `--duration`) |
//! | *(op variant)* | Each `Op` from the workload generator           | See `op_payload` below — `id`/`ids`/etc. depending on variant |
//! | `Restart`      | Mid-run close+reopen+verify                     | `pre_points`, `pre_segments` |
//! | `LiveVerify`   | End-of-run scroll vs model, before reload       | `model_points`, `engine_points`, `segments`, `optimized_points`, `extra`, `missing` |
//! | `ReloadVerify` | End-of-run scroll vs model, after final reload  | `model_points`, `engine_points`, `extra`, `missing` |
//!
//! `extra` = ids the engine has that the model doesn't.
//! `missing` = ids the model has that the engine doesn't.
//! When the run is clean both arrays are empty.
//!
//! # Reading the file (agent workflow)
//!
//! 1. Read the first line → run config (seed, optimizer mode, etc.).
//! 2. Read the last 1–2 lines → outcome. `ReloadVerify` with empty `extra`/`missing`
//!    means the run finished clean. Non-empty arrays = the bug surface.
//! 3. For each id in `extra`/`missing`, grep the file for that id to reconstruct
//!    its operation history. The last op for an id in `missing` should tell you
//!    why the model expected it to exist; the last op for an id in `extra`
//!    should tell you why the model expected it gone.
//! 4. If the file ends with no `ReloadVerify` line, the panic happened during
//!    live verification or earlier — the trailing op events are the suspects.
//!
//! # Run-to-run comparison
//!
//! Two runs with the same seed and same args should produce byte-identical op
//! lines. Differences in `LiveVerify`/`ReloadVerify`/`Restart` fields (segment
//! counts, optimizer counters) point at engine-side non-determinism. Use
//! `diff -u trace.log other.log` and ignore the verify lines if you only care
//! about workload determinism.

use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::Duration;

use fs_err::File;
use segment::types::PointIdType;
use serde_json::{Value, json};

use super::op::{NamedVectors, Op};

pub(super) struct Trace {
    writer: BufWriter<File>,
}

impl Trace {
    pub(super) fn create(path: &Path) -> std::io::Result<Self> {
        Ok(Self {
            writer: BufWriter::new(File::create(path)?),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn header(
        &mut self,
        seed: u64,
        op_num: usize,
        shard_count: u32,
        id_pool: u64,
        disable_optimizer: bool,
        max_segment_size_kb: usize,
        indexing_threshold_kb: usize,
        flush_interval_sec: u64,
        restart_probability: f64,
        swarm_interval: usize,
        enable_force_off: bool,
        duration: Option<Duration>,
    ) {
        self.write(&json!({
            "kind": "Header",
            "seed": seed,
            "op_num": op_num,
            "shard_count": shard_count,
            "id_pool": id_pool,
            "disable_optimizer": disable_optimizer,
            "max_segment_size_kb": max_segment_size_kb,
            "indexing_threshold_kb": indexing_threshold_kb,
            "flush_interval_sec": flush_interval_sec,
            "restart_probability": restart_probability,
            "swarm_interval": swarm_interval,
            "enable_force_off": enable_force_off,
            // null in op-count mode; seconds when the run is time-bounded (`--duration`).
            "duration_sec": duration.map(|d| d.as_secs()),
        }));
    }

    /// Records a swarm-testing config (the random subset of enabled ops) effective from op
    /// `tick` until the next redraw. Logged with its tick so a failure is attributable to the
    /// config that was live when it happened.
    pub(super) fn swarm(&mut self, tick: usize, enabled_ops: &[&str]) {
        self.write(&json!({
            "op": tick,
            "kind": "Swarm",
            "enabled_ops": enabled_ops,
        }));
    }

    pub(super) fn op(&mut self, tick: usize, op: &Op) {
        // Insert `op` and `kind` first so they lead every line — agents and humans
        // scanning the trace identify the event from the leftmost fields.
        let mut map = serde_json::Map::new();
        map.insert("op".to_string(), json!(tick));
        map.insert("kind".to_string(), json!(op.kind()));
        if let Value::Object(payload) = op_payload(op) {
            for (k, v) in payload {
                map.insert(k, v);
            }
        }
        self.write(&Value::Object(map));
    }

    pub(super) fn restart(&mut self, tick: usize, pre_points: usize, pre_segments: usize) {
        self.write(&json!({
            "op": tick,
            "kind": "Restart",
            "pre_points": pre_points,
            "pre_segments": pre_segments,
        }));
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn live_verify(
        &mut self,
        tick: usize,
        model_points: usize,
        engine_points: usize,
        segments: usize,
        optimized_points: usize,
        extra: &[PointIdType],
        missing: &[PointIdType],
    ) {
        self.write(&json!({
            "op": tick,
            "kind": "LiveVerify",
            "model_points": model_points,
            "engine_points": engine_points,
            "segments": segments,
            "optimized_points": optimized_points,
            "extra": extra,
            "missing": missing,
        }));
    }

    pub(super) fn reload_verify(
        &mut self,
        tick: usize,
        model_points: usize,
        engine_points: usize,
        extra: &[PointIdType],
        missing: &[PointIdType],
    ) {
        self.write(&json!({
            "op": tick,
            "kind": "ReloadVerify",
            "model_points": model_points,
            "engine_points": engine_points,
            "extra": extra,
            "missing": missing,
        }));
    }

    fn write(&mut self, value: &Value) {
        if let Ok(line) = serde_json::to_string(value) {
            let _ = writeln!(self.writer, "{line}");
            let _ = self.writer.flush();
        }
    }
}

fn op_payload(op: &Op) -> Value {
    match op {
        Op::Upsert(id, vecs, _payload) => json!({
            "id": id,
            "vectors": vector_names(vecs),
        }),
        Op::UpsertBatch(points) => json!({
            "ids": points.iter().map(|(id, _, _)| id).collect::<Vec<_>>(),
        }),
        Op::Delete(ids) => json!({ "ids": ids }),
        Op::DeleteByFilter(num) => json!({ "num": num }),
        Op::SetPayload(ids, _payload) => json!({ "ids": ids }),
        Op::OverwritePayload(ids, _payload) => json!({ "ids": ids }),
        Op::DeletePayload(ids, keys) => json!({
            "ids": ids,
            "keys": keys.iter().map(|k| k.to_string()).collect::<Vec<_>>(),
        }),
        Op::ClearPayload(ids) => json!({ "ids": ids }),
        Op::CreateIndex(field, _) => json!({ "field": field.to_string() }),
        Op::DropIndex(field) => json!({ "field": field.to_string() }),
        Op::RetrieveRandom(ids) => json!({ "ids": ids }),
        Op::RetrieveExisting(ids) => json!({ "ids": ids }),
        Op::CountByNum(num) => json!({ "num": num }),
        Op::Search {
            vector_name,
            limit,
            exact,
            filter_num,
            filter_url_prefix,
            ..
        } => json!({
            "vector_name": vector_name,
            "limit": limit,
            "exact": exact,
            "filter_num": filter_num,
            "filter_url_prefix": filter_url_prefix,
        }),
        Op::Query {
            vector_name,
            limit,
            exact,
            filter_num,
            filter_url_prefix,
            ..
        } => json!({
            "vector_name": vector_name,
            "limit": limit,
            "exact": exact,
            "filter_num": filter_num,
            "filter_url_prefix": filter_url_prefix,
        }),
        Op::UpsertConditional {
            points,
            condition_num,
            mode,
        } => json!({
            "ids": points.iter().map(|(id, _, _)| id).collect::<Vec<_>>(),
            "condition_num": condition_num,
            "mode": mode,
        }),
        Op::UpdateVectors {
            points,
            condition_num,
        } => json!({
            "ids": points.iter().map(|(id, _)| id).collect::<Vec<_>>(),
            "condition_num": condition_num,
        }),
        Op::DeleteVectors { ids, names } => json!({
            "ids": ids,
            "names": names,
        }),
        Op::DeleteVectorsByFilter { num, names } => json!({
            "num": num,
            "names": names,
        }),
        Op::ScrollFilteredByNum(num) => json!({ "num": num }),
        Op::CountByTag(tag) => json!({ "tag": tag }),
        Op::CountByUrlPrefix(prefix) => json!({ "url_prefix": prefix }),
        Op::ScrollFilteredByTag(tag) => json!({ "tag": tag }),
        Op::ScrollFilteredByUrlPrefix(prefix) => json!({ "url_prefix": prefix }),
        Op::ScrollOrdered(dir) => json!({ "direction": dir }),
        Op::Recommend {
            positive,
            negative,
            limit,
            strategy,
            vector_name,
        } => json!({
            "positive": positive,
            "negative": negative,
            "limit": limit,
            "strategy": strategy,
            "vector_name": vector_name,
        }),
        Op::CreateVectorName { name, .. } => json!({ "name": name }),
        Op::DeleteVectorName(name) => json!({ "name": name }),
        Op::SetPayloadByFilter { num, .. } => json!({ "num": num }),
        Op::OverwritePayloadByFilter { num, .. } => json!({ "num": num }),
        Op::DeletePayloadByFilter { num, keys } => json!({
            "num": num,
            "keys": keys.iter().map(|k| k.to_string()).collect::<Vec<_>>(),
        }),
        Op::ClearPayloadByFilter(num) => json!({ "num": num }),
        Op::Facet {
            key,
            filter_num,
            filter_url_prefix,
        } => json!({
            "key": key,
            "filter_num": filter_num,
            "filter_url_prefix": filter_url_prefix,
        }),
        Op::SetPayloadByKey { ids, key, .. } => json!({
            "ids": ids,
            "key": key.to_string(),
        }),
        Op::RetrieveSelective {
            ids,
            with_payload,
            with_vector,
        } => json!({
            "ids": ids,
            "with_payload": format!("{with_payload:?}"),
            "with_vector": format!("{with_vector:?}"),
        }),
        Op::ScrollPaged { limit, filter } => json!({
            "limit": limit,
            "filter": format!("{filter:?}"),
        }),
        Op::QueryFusion {
            prefetches,
            fusion,
            limit,
            filter_num,
            filter_url_prefix,
        } => json!({
            "fusion": format!("{fusion:?}"),
            "limit": limit,
            "filter_num": filter_num,
            "filter_url_prefix": filter_url_prefix,
            "prefetches": prefetches
                .iter()
                .map(|p| json!({
                    "vector_name": p.vector_name.as_str(),
                    "limit": p.limit,
                    "filter_num": p.filter_num,
                }))
                .collect::<Vec<_>>(),
        }),
    }
}

fn vector_names(vecs: &NamedVectors) -> Vec<&str> {
    vecs.keys().map(String::as_str).collect()
}
