//! Target-independent core: open a read-only edge shard over [`MemFs`] and query it.
//!
//! Kept free of `wasm-bindgen` so it can be exercised by a native test (see the tests module at the
//! bottom) — the wasm layer in [`crate::lib`] only adds the JS boundary on top.

use std::path::{Path, PathBuf};

use edge::{
    DEFAULT_VECTOR_NAME, EdgeShardRead, Filter, NamedQuery, QueryEnum, ReadOnlyEdgeShard, Record,
    ScoredPoint, ScrollRequest, SearchRequest, ShardInfo,
};
use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::vectors::{DenseVector, VectorInternal};
use shard::files::WAL_PATH;

use crate::mem_fs::{MemFile, MemFs};

/// A read-only edge shard whose segments live entirely in the linear memory.
pub struct MemEdgeShard {
    shard: ReadOnlyEdgeShard<MemFile>,
}

impl MemEdgeShard {
    /// Open the shard rooted at `path` within `fs`.
    ///
    /// Segments are discovered through the leader's segment manifest, which must therefore be
    /// among the files loaded into `fs` — same contract as any other read-only follower.
    pub fn open(fs: MemFs, path: &Path) -> OperationResult<Self> {
        let shard = ReadOnlyEdgeShard::<MemFile>::open(fs, path, None, None)?;
        Ok(MemEdgeShard { shard })
    }

    /// Shard-level counters and the derived config.
    pub fn info(&self) -> OperationResult<ShardInfo> {
        self.shard.info()
    }

    /// Nearest-neighbour search for a dense `vector` in `vector_name`.
    pub fn search(
        &self,
        vector_name: Option<&str>,
        vector: DenseVector,
        limit: usize,
        filter: Option<Filter>,
        with_payload: bool,
    ) -> OperationResult<Vec<ScoredPoint>> {
        let name = vector_name.unwrap_or(DEFAULT_VECTOR_NAME).to_owned();
        let query = QueryEnum::Nearest(NamedQuery {
            query: VectorInternal::Dense(vector),
            using: Some(name),
        });

        let request = SearchRequest {
            filter,
            with_payload: Some(with_payload.into()),
            ..SearchRequest::new(query, limit)
        };

        self.shard.search(request)
    }

    /// Paginate over points, optionally filtered.
    pub fn scroll(
        &self,
        limit: usize,
        filter: Option<Filter>,
        with_payload: bool,
    ) -> OperationResult<Vec<Record>> {
        let request = ScrollRequest {
            limit: Some(limit),
            filter,
            with_payload: Some(with_payload.into()),
            ..ScrollRequest::new()
        };

        let (records, _next_offset) = self.shard.scroll(request)?;
        Ok(records)
    }
}

/// Decide which listed keys to download, and what shard-relative path each becomes.
///
/// The shard is opened at [`Path::new("")`], so a key `collection/0/segments/<uuid>/…` listed
/// under prefix `collection/0` becomes `segments/<uuid>/…`. Keys outside the prefix, and the
/// prefix "directory" marker itself, are dropped.
///
/// The leader's write-ahead log is dropped too. A read-only follower never replays it, and the
/// segment files are pre-allocated to their configured capacity — tens of megabytes of zeroes that
/// would otherwise dominate both the download and the resident set.
pub fn shard_files(keys: impl IntoIterator<Item = String>, prefix: &str) -> Vec<(PathBuf, String)> {
    let prefix = prefix.trim_matches('/');

    keys.into_iter()
        .filter_map(|key| {
            let rest = if prefix.is_empty() {
                key.as_str()
            } else {
                key.strip_prefix(prefix)?.trim_start_matches('/')
            };

            if rest.is_empty() || rest.starts_with(&format!("{WAL_PATH}/")) {
                return None;
            }

            Some((PathBuf::from(rest), key.clone()))
        })
        .collect()
}

/// Turn a shard-open failure into a message worth showing a user.
pub fn describe_open_error(err: &OperationError) -> String {
    format!("failed to open read-only edge shard: {err}")
}

/// Render a shard's counters as JSON.
///
/// The result types are internal to `edge` and do not implement [`serde::Serialize`], so every
/// crossing of the JS boundary goes through one of these renderers rather than deriving it.
pub fn info_json(info: &ShardInfo) -> serde_json::Value {
    let ShardInfo {
        segments_count,
        points_count,
        indexed_vectors_count,
        payload_schema,
    } = info;

    serde_json::json!({
        "segments_count": segments_count,
        "points_count": points_count,
        "indexed_vectors_count": indexed_vectors_count,
        "payload_schema": payload_schema.keys().map(ToString::to_string).collect::<Vec<_>>(),
    })
}

/// Render a search hit as JSON.
pub fn scored_point_json(point: &ScoredPoint) -> serde_json::Value {
    serde_json::json!({
        "id": point.id,
        "version": point.version,
        "score": point.score,
        "payload": point.payload,
        "vector": point.vector.as_ref().map(|vector| format!("{vector:?}")),
    })
}

/// Render a scrolled record as JSON.
pub fn record_json(record: &Record) -> serde_json::Value {
    serde_json::json!({
        "id": record.id,
        "payload": record.payload,
        "vector": record.vector.as_ref().map(|vector| format!("{vector:?}")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_files_makes_shard_relative_paths() {
        let keys = [
            "collection/0/segments/abc/segment.json".to_string(),
            "collection/0/segments_manifest.json".to_string(),
            "collection/0/wal/open-1".to_string(),
            "collection/0".to_string(),
            "other/thing".to_string(),
        ];

        let stripped = shard_files(keys, "collection/0");
        let paths: Vec<_> = stripped.iter().map(|(path, _)| path.clone()).collect();

        assert_eq!(
            paths,
            vec![
                PathBuf::from("segments/abc/segment.json"),
                PathBuf::from("segments_manifest.json"),
            ]
        );
    }

    #[test]
    fn shard_files_passes_through_empty_prefix() {
        let keys = ["segments/abc/segment.json".to_string()];
        let stripped = shard_files(keys, "");

        assert_eq!(stripped[0].0, PathBuf::from("segments/abc/segment.json"));
    }
}
