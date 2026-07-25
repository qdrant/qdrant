//! A read-only Qdrant edge shard, compiled to `wasm32-unknown-unknown` and queried straight from
//! object storage.
//!
//! # Why it is shaped this way
//!
//! [`UniversalRead`](common::universal_io::UniversalRead) — the interface every segment component
//! reads through — is *synchronous*. Native blob backends satisfy that by blocking a thread on a
//! Tokio runtime (`io_bridge`), which a browser cannot do: `fetch` only resolves when the JS event
//! loop runs, so blocking the thread that would run it deadlocks.
//!
//! So the fetching is hoisted out of the read path entirely. [`open`] is `async`: it lists the
//! prefix, downloads every object, and parks the bytes in [`MemFs`](mem_fs::MemFs). Only then is
//! the shard opened — and from that point on every read is a slice of linear memory and the
//! synchronous interface is honest.
//!
//! The cost is that the whole shard is resident, capped by the 32-bit address space. Lazy range
//! reads would need either a Web Worker blocking on `Atomics.wait` against a fetching main thread,
//! or OPFS sync access handles; both keep this same `UniversalRead` impl and only replace where
//! the bytes come from.

pub mod mem_fs;
#[cfg(target_arch = "wasm32")]
pub mod object_store;
pub mod shard;

#[cfg(target_arch = "wasm32")]
mod bindings {
    use edge::{Filter, ScoredPoint};
    use wasm_bindgen::prelude::*;

    use crate::mem_fs::MemFs;
    use crate::object_store;
    use crate::shard::{
        MemEdgeShard, describe_open_error, info_json, record_json, scored_point_json, shard_files,
    };

    /// Install a panic hook and a `console` logger. Safe to call more than once.
    #[wasm_bindgen(start)]
    pub fn start() {
        console_error_panic_hook::set_once();
        let _ = console_log::init_with_level(log::Level::Info);
    }

    /// An opened shard, held on the JS side.
    #[wasm_bindgen]
    pub struct EdgeShard {
        inner: MemEdgeShard,
        bytes_loaded: u64,
        files_loaded: usize,
    }

    /// Download every object under `prefix` from the bucket at `base_url` and open a read-only
    /// edge shard over them.
    ///
    /// `base_url` is the bucket endpoint (`https://s3.example.com/my-bucket`) and `prefix` the key
    /// prefix the shard was written under (`collection/0`). The bucket must permit anonymous reads
    /// and send CORS headers that allow this origin.
    #[wasm_bindgen]
    pub async fn open(base_url: String, prefix: String) -> Result<EdgeShard, JsError> {
        let listing = object_store::list(&base_url, &prefix)
            .await
            .map_err(|err| JsError::new(&err))?;

        log::info!(
            "listed {} object(s) under {prefix}, {} byte(s) total",
            listing.len(),
            listing.iter().map(|object| object.size).sum::<u64>(),
        );

        let keys = listing.into_iter().map(|object| object.key);
        let base = base_url.trim_end_matches('/').to_owned();

        let mut files = Vec::new();
        for (path, key) in shard_files(keys, &prefix) {
            let bytes = object_store::get(&format!("{base}/{key}"))
                .await
                .map_err(|err| JsError::new(&err))?;
            files.push((path, bytes));
        }

        let files_loaded = files.len();
        let fs = MemFs::new(files);
        let bytes_loaded = fs.total_len();

        let inner = MemEdgeShard::open(fs, std::path::Path::new(""))
            .map_err(|err| JsError::new(&describe_open_error(&err)))?;

        Ok(EdgeShard {
            inner,
            bytes_loaded,
            files_loaded,
        })
    }

    #[wasm_bindgen]
    impl EdgeShard {
        /// Bytes downloaded into linear memory at open.
        #[wasm_bindgen(getter)]
        pub fn bytes_loaded(&self) -> u64 {
            self.bytes_loaded
        }

        /// Number of objects downloaded at open.
        #[wasm_bindgen(getter)]
        pub fn files_loaded(&self) -> usize {
            self.files_loaded
        }

        /// Shard counters and derived config, as JSON.
        pub fn info(&self) -> Result<JsValue, JsError> {
            let info = self
                .inner
                .info()
                .map_err(|err| JsError::new(&err.to_string()))?;
            to_json(&info_json(&info))
        }

        /// Nearest-neighbour search.
        ///
        /// `vector_name` selects the named vector (`null` for the unnamed one) and `filter` is a
        /// Qdrant filter as JSON (`null` for none). Returns the scored points as JSON.
        pub fn search(
            &self,
            vector: Vec<f32>,
            limit: usize,
            vector_name: Option<String>,
            filter: JsValue,
            with_payload: bool,
        ) -> Result<JsValue, JsError> {
            let points = self
                .inner
                .search(
                    vector_name.as_deref(),
                    vector,
                    limit,
                    parse_filter(filter)?,
                    with_payload,
                )
                .map_err(|err| JsError::new(&err.to_string()))?;

            log_scores(&points);
            let rendered: Vec<_> = points.iter().map(scored_point_json).collect();
            to_json(&rendered)
        }

        /// Paginate over points, optionally filtered. Returns the records as JSON.
        pub fn scroll(
            &self,
            limit: usize,
            filter: JsValue,
            with_payload: bool,
        ) -> Result<JsValue, JsError> {
            let records = self
                .inner
                .scroll(limit, parse_filter(filter)?, with_payload)
                .map_err(|err| JsError::new(&err.to_string()))?;

            let rendered: Vec<_> = records.iter().map(record_json).collect();
            to_json(&rendered)
        }
    }

    fn parse_filter(filter: JsValue) -> Result<Option<Filter>, JsError> {
        if filter.is_null() || filter.is_undefined() {
            return Ok(None);
        }

        let json: serde_json::Value = serde_wasm_value(filter)?;
        serde_json::from_value(json)
            .map(Some)
            .map_err(|err| JsError::new(&format!("invalid filter: {err}")))
    }

    /// Round-trip a `JsValue` through `JSON.stringify` into `serde_json`.
    ///
    /// Avoids a `serde-wasm-bindgen` dependency for the one place a JS object comes *in*.
    fn serde_wasm_value(value: JsValue) -> Result<serde_json::Value, JsError> {
        let text = js_sys::JSON::stringify(&value)
            .map_err(|_| JsError::new("value is not JSON-serializable"))?;
        let text: String = text.into();

        serde_json::from_str(&text).map_err(|err| JsError::new(&format!("invalid JSON: {err}")))
    }

    fn to_json<T: serde::Serialize>(value: &T) -> Result<JsValue, JsError> {
        let text = serde_json::to_string(value).map_err(|err| JsError::new(&format!("{err}")))?;
        js_sys::JSON::parse(&text).map_err(|_| JsError::new("result was not valid JSON"))
    }

    fn log_scores(points: &[ScoredPoint]) {
        log::info!(
            "search returned {} result(s){}",
            points.len(),
            points
                .first()
                .map(|point| format!(", best score {}", point.score))
                .unwrap_or_default(),
        );
    }
}

#[cfg(target_arch = "wasm32")]
pub use bindings::*;
