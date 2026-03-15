use std::collections::HashMap;

use common::mmap::MULTI_MMAP_SUPPORT_CHECK_RESULT;
use edge::info::ShardInfo;
use edge::EdgeShard;
use segment::data_types::vectors::{
    NamedQuery, VectorInternal, VectorStructInternal, DEFAULT_VECTOR_NAME,
};
use segment::types::{
    Distance, ExtendedPointId, Filter, Indexes, PayloadStorageType, SegmentConfig,
    VectorDataConfig, VectorStorageType, WithPayloadInterface, WithVector,
};
use serde::{Deserialize, Serialize};
use shard::count::CountRequestInternal;
use shard::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted, VectorStructPersisted,
};
use shard::operations::CollectionUpdateOperations;
use shard::query::query_enum::QueryEnum;
use shard::retrieve::record_internal::RecordInternal;
use shard::scroll::ScrollRequestInternal;
use shard::search::CoreSearchRequest;

pub struct WasmEdgeShard {
    shard: EdgeShard,
}

#[derive(Serialize)]
pub struct SearchResult {
    pub id: u64,
    pub score: f32,
}

#[derive(Serialize)]
pub struct RecordOut {
    pub id: u64,
    pub payload: Option<segment::types::Payload>,
    pub vector: Option<Vec<f32>>,
}

#[derive(Serialize)]
pub struct ScrollResult {
    pub points: Vec<RecordOut>,
    pub next_page_offset: Option<u64>,
}

#[derive(Deserialize)]
pub struct ScrollRequest {
    pub offset: Option<u64>,
    pub limit: Option<usize>,
    pub filter: Option<Filter>,
    pub with_payload: Option<bool>,
    pub with_vector: Option<bool>,
}

fn record_to_out(record: RecordInternal) -> RecordOut {
    let id = match record.id {
        ExtendedPointId::NumId(n) => n,
        _ => 0,
    };
    let vector = record.vector.and_then(|v| match v {
        VectorStructInternal::Single(vec) => Some(vec),
        _ => None,
    });
    RecordOut {
        id,
        payload: record.payload,
        vector,
    }
}

impl WasmEdgeShard {
    pub fn new(path: String) -> Result<Self, String> {
        init_wasm();
        let path = std::path::Path::new(&path);
        EdgeShard::load(path, None)
            .map(|shard| WasmEdgeShard { shard })
            .map_err(|e| format!("Failed to load shard: {e}"))
    }

    pub fn upsert(
        &mut self,
        id: u64,
        vector: Vec<f32>,
        payload: Option<segment::types::Payload>,
    ) -> Result<(), String> {
        let point = PointStructPersisted {
            id: ExtendedPointId::NumId(id),
            vector: VectorStructPersisted::Single(vector),
            payload,
        };
        let op = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
            PointInsertOperationsInternal::PointsList(vec![point]),
        ));
        self.shard.update(op).map_err(|e| e.to_string())
    }

    pub fn search(&self, vector: Vec<f32>, top_k: usize) -> Result<Vec<SearchResult>, String> {
        let request = CoreSearchRequest {
            query: QueryEnum::Nearest(NamedQuery {
                query: VectorInternal::Dense(vector.into()),
                using: None,
            }),
            filter: None,
            params: None,
            limit: top_k,
            offset: 0,
            with_payload: None,
            with_vector: None,
            score_threshold: None,
        };
        let results = self.shard.search(request).map_err(|e| e.to_string())?;
        Ok(results
            .into_iter()
            .map(|p| SearchResult {
                id: match p.id {
                    ExtendedPointId::NumId(n) => n,
                    _ => 0,
                },
                score: p.score,
            })
            .collect())
    }

    pub fn info(&self) -> ShardInfo {
        self.shard.info()
    }

    pub fn retrieve(
        &self,
        ids: Vec<u64>,
        with_payload: bool,
        with_vector: bool,
    ) -> Result<Vec<RecordOut>, String> {
        let point_ids: Vec<ExtendedPointId> =
            ids.iter().map(|&n| ExtendedPointId::NumId(n)).collect();
        let records = self
            .shard
            .retrieve(
                &point_ids,
                Some(WithPayloadInterface::Bool(with_payload)),
                Some(WithVector::Bool(with_vector)),
            )
            .map_err(|e| e.to_string())?;
        Ok(records.into_iter().map(record_to_out).collect())
    }

    pub fn count(&self, filter: Option<Filter>, exact: bool) -> Result<usize, String> {
        self.shard
            .count(CountRequestInternal { filter, exact })
            .map_err(|e| e.to_string())
    }

    pub fn scroll(&self, req: ScrollRequest) -> Result<ScrollResult, String> {
        let scroll_req = ScrollRequestInternal {
            offset: req.offset.map(ExtendedPointId::NumId),
            limit: req.limit,
            filter: req.filter,
            with_payload: req.with_payload.map(WithPayloadInterface::Bool),
            with_vector: req
                .with_vector
                .map(WithVector::Bool)
                .unwrap_or(WithVector::Bool(false)),
            order_by: None,
        };
        let (records, next_offset) = self.shard.scroll(scroll_req).map_err(|e| e.to_string())?;
        let next_page_offset = next_offset.map(|id| match id {
            ExtendedPointId::NumId(n) => n,
            _ => 0,
        });
        Ok(ScrollResult {
            points: records.into_iter().map(record_to_out).collect(),
            next_page_offset,
        })
    }
}

fn init_wasm() {
    // On WASM/WASI, multiple memory maps to the same file are not supported.
    // Set this once so MULTI_MMAP_IS_SUPPORTED correctly returns false.
    let _ = MULTI_MMAP_SUPPORT_CHECK_RESULT.set(false);
}

pub fn build_shard_segment(path: String) -> Result<(), String> {
    init_wasm();
    let path = std::path::Path::new(&path);
    let config = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: 4,
                distance: Distance::Cosine,
                storage_type: VectorStorageType::ChunkedMmap,
                index: Indexes::Plain {},
                quantization_config: None,
                multivector_config: None,
                datatype: None,
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: PayloadStorageType::Mmap,
    };
    EdgeShard::load(path, Some(config))
        .map(|_| ())
        .map_err(|e| format!("Build error: {e}"))
}

static mut RESULT_BUFFER: Option<String> = None;

fn set_result(res: String) -> *mut u8 {
    unsafe {
        RESULT_BUFFER = Some(res);
        RESULT_BUFFER.as_mut().unwrap().as_mut_ptr()
    }
}

#[no_mangle]
pub extern "C" fn get_result_ptr() -> *mut u8 {
    unsafe {
        RESULT_BUFFER
            .as_mut()
            .map(|s| s.as_mut_ptr())
            .unwrap_or(std::ptr::null_mut())
    }
}

#[no_mangle]
pub extern "C" fn get_result_len() -> usize {
    unsafe { RESULT_BUFFER.as_ref().map(|s| s.len()).unwrap_or(0) }
}

#[no_mangle]
pub extern "C" fn shard_build(path_ptr: *const u8, path_len: usize) -> bool {
    let path = unsafe {
        String::from_utf8_lossy(std::slice::from_raw_parts(path_ptr, path_len)).into_owned()
    };
    match build_shard_segment(path) {
        Ok(_) => true,
        Err(e) => {
            set_result(e);
            false
        }
    }
}

#[no_mangle]
pub extern "C" fn shard_new(path_ptr: *const u8, path_len: usize) -> *mut WasmEdgeShard {
    let path = unsafe {
        String::from_utf8_lossy(std::slice::from_raw_parts(path_ptr, path_len)).into_owned()
    };
    match WasmEdgeShard::new(path) {
        Ok(shard) => Box::into_raw(Box::new(shard)),
        Err(e) => {
            set_result(e);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn shard_upsert(
    shard: *mut WasmEdgeShard,
    id: u64,
    vec_ptr: *const f32,
    vec_len: usize,
    payload_ptr: *const u8,
    payload_len: usize,
) -> bool {
    let shard = unsafe { &mut *shard };
    let vector = unsafe { std::slice::from_raw_parts(vec_ptr, vec_len).to_vec() };
    let payload = if payload_ptr.is_null() || payload_len == 0 {
        None
    } else {
        let s = unsafe { std::slice::from_raw_parts(payload_ptr, payload_len) };
        serde_json::from_slice(s).ok()
    };
    match shard.upsert(id, vector, payload) {
        Ok(_) => true,
        Err(e) => {
            set_result(e);
            false
        }
    }
}

#[no_mangle]
pub extern "C" fn shard_search(
    shard: *mut WasmEdgeShard,
    vec_ptr: *const f32,
    vec_len: usize,
    top_k: usize,
) -> *mut u8 {
    let shard = unsafe { &*shard };
    let vector = unsafe { std::slice::from_raw_parts(vec_ptr, vec_len).to_vec() };
    match shard.search(vector, top_k) {
        Ok(results) => set_result(serde_json::to_string(&results).unwrap()),
        Err(e) => {
            set_result(e);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn shard_info(shard: *mut WasmEdgeShard) -> *mut u8 {
    let shard = unsafe { &*shard };
    set_result(serde_json::to_string(&shard.info()).unwrap())
}

#[no_mangle]
pub extern "C" fn shard_retrieve(
    shard: *mut WasmEdgeShard,
    ids_ptr: *const u64,
    ids_len: usize,
    with_payload: bool,
    with_vector: bool,
) -> *mut u8 {
    let shard = unsafe { &*shard };
    let ids = unsafe { std::slice::from_raw_parts(ids_ptr, ids_len).to_vec() };
    match shard.retrieve(ids, with_payload, with_vector) {
        Ok(res) => set_result(serde_json::to_string(&res).unwrap()),
        Err(e) => {
            set_result(e);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn shard_count(
    shard: *mut WasmEdgeShard,
    filter_ptr: *const u8,
    filter_len: usize,
    exact: bool,
) -> *mut u8 {
    let shard = unsafe { &*shard };
    let filter = if filter_ptr.is_null() || filter_len == 0 {
        None
    } else {
        let s = unsafe { std::slice::from_raw_parts(filter_ptr, filter_len) };
        serde_json::from_slice(s).unwrap_or(None)
    };
    match shard.count(filter, exact) {
        Ok(count) => set_result(count.to_string()),
        Err(e) => {
            set_result(e);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn shard_scroll(
    shard: *mut WasmEdgeShard,
    req_ptr: *const u8,
    req_len: usize,
) -> *mut u8 {
    let shard = unsafe { &*shard };
    let s = unsafe { std::slice::from_raw_parts(req_ptr, req_len) };
    let req: ScrollRequest = match serde_json::from_slice(s) {
        Ok(r) => r,
        Err(e) => {
            set_result(format!("Scroll request parse error: {e}"));
            return std::ptr::null_mut();
        }
    };
    match shard.scroll(req) {
        Ok(res) => set_result(serde_json::to_string(&res).unwrap()),
        Err(e) => {
            set_result(e);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn qdrant_malloc(size: usize) -> *mut u8 {
    let layout = std::alloc::Layout::from_size_align(size, 8).unwrap();
    unsafe { std::alloc::alloc(layout) }
}

#[no_mangle]
pub extern "C" fn qdrant_free(ptr: *mut u8, size: usize) {
    let layout = std::alloc::Layout::from_size_align(size, 8).unwrap();
    unsafe { std::alloc::dealloc(ptr, layout) }
}
