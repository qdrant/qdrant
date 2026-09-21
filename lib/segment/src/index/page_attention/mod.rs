//! Experimental immutable attention index. Results encode coordinates in the score channel.
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, atomic::AtomicBool};

use atomic_refcell::AtomicRefCell;
use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Random;
use common::types::{PointOffsetType, ScoredPointOffset, TelemetryDetail};
use page_attention::pages::PagesHead;
use page_attention::pagesearch::{self, ExactRowsSource, Parameters};
use page_attention::tq4::Rotation;
use rayon::prelude::*;
use sparse::common::types::DimId;

use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::data_types::query_context::VectorQueryContext;
use crate::data_types::vectors::{QueryVector, VectorInternal, VectorRef};
use crate::id_tracker::{IdTrackerEnum, IdTrackerRead};
use crate::index::vector_index_base::{VectorIndex, VectorIndexRead};
use crate::telemetry::VectorIndexSearchesTelemetry;
use crate::types::{
    Distance, Filter, PageAttentionConfig, PointIdType, SearchParams, VectorDataConfig,
};
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead};

const EXTENSIONS: [&str; 6] = ["meta", "pages", "side", "graph", "summary", "inverse"];

struct Originals<'a> {
    storage: &'a VectorStorageEnum,
    query: &'a [f32],
}
impl ExactRowsSource for Originals<'_> {
    fn dot_key(&mut self, pos: u32) -> f32 {
        let row = self.storage.get_vector::<Random>(pos);
        let VectorRef::Dense(row) = row.as_vec_ref() else {
            unreachable!("validated dense storage")
        };
        page_attention::rescore::dot(self.query, &row[..self.query.len()])
    }
    fn value(&mut self, pos: u32, out: &mut [f32]) {
        let row = self.storage.get_vector::<Random>(pos);
        let VectorRef::Dense(row) = row.as_vec_ref() else {
            unreachable!("validated dense storage")
        };
        out.copy_from_slice(&row[self.query.len()..]);
    }
    fn will_need(&mut self, positions: &[u32]) {
        for &pos in positions {
            // Raw mapped bytes avoid decoding/faulting the row before the IO
            // hint. K and V share this range in concatenated original storage.
            let _ = self
                .storage
                .with_vector_bytes_opt::<Random, _>(pos, page_attention::rescore::prefetch);
        }
    }
}

pub struct PageAttentionIndex {
    head: PagesHead,
    rotation: Rotation,
    config: PageAttentionConfig,
    files: Vec<PathBuf>,
    id_tracker: Arc<AtomicRefCell<IdTrackerEnum>>,
    vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
}

impl std::fmt::Debug for PageAttentionIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageAttentionIndex")
            .field("config", &self.config)
            .finish()
    }
}

impl PageAttentionIndex {
    pub fn open(
        path: &Path,
        config: &PageAttentionConfig,
        vector_config: &VectorDataConfig,
        id_tracker: Arc<AtomicRefCell<IdTrackerEnum>>,
        vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
        build: bool,
    ) -> OperationResult<Self> {
        if !matches!(config.head_dim, 128 | 256)
            || vector_config.size != config.head_dim * 2
            || vector_config.distance != Distance::Dot
            || vector_config.multivector_config.is_some()
            || vector_config.quantization_config.is_some()
        {
            return Err(OperationError::validation_error(
                "page_attention requires dense concatenated K+V, head_dim 128 or 256, Dot distance and no Qdrant quantization",
            ));
        }
        let stem = format!("l{:04}h{:04}", config.layer, config.head);
        let files: Vec<_> = EXTENSIONS
            .iter()
            .map(|ext| path.join(format!("{stem}.{ext}")))
            .collect();
        if build {
            let source = Path::new(&config.generation);
            let manifest = std::fs::read(
                source
                    .parent()
                    .ok_or_else(|| {
                        OperationError::validation_error("generation has no session directory")
                    })?
                    .join("manifest.json"),
            )?;
            let binding = std::fs::read_to_string(source.join("source.fnv"))?;
            if binding.trim() != format!("{:016x}", page_attention::tq4::fnv1a64(&manifest)) {
                return Err(OperationError::validation_error(
                    "page_attention generation source manifest changed",
                ));
            }
            let manifest: serde_json::Value = serde_json::from_slice(&manifest)
                .map_err(|e| OperationError::validation_error(e.to_string()))?;
            if manifest["session_id"].as_str() != Some(config.session_id.as_str())
                || manifest["spec"]["head_dim"].as_u64() != Some(config.head_dim as u64)
            {
                return Err(OperationError::validation_error(
                    "page_attention session id or dimension does not match the generation",
                ));
            }
            std::fs::create_dir_all(path)?;
            for file in &files {
                std::fs::copy(
                    Path::new(&config.generation).join(file.file_name().unwrap()),
                    file,
                )?;
            }
        }
        let rotation = Rotation::for_session(&config.session_id, config.head_dim);
        let head = PagesHead::load(path, config.layer, config.head, &rotation)?;
        if head.n < head.dim + 1 {
            return Err(OperationError::validation_error(
                "page_attention requires at least head_dim + 1 live points for its score channel",
            ));
        }
        let index = Self {
            head,
            rotation,
            config: config.clone(),
            files,
            id_tracker,
            vector_storage,
        };
        index.validate_count()?;
        let tracker = index.id_tracker.borrow();
        for id in 0..index.head.n as u32 {
            if tracker.external_id(id) != Some(PointIdType::NumId(id as u64)) {
                return Err(OperationError::validation_error(
                    "page_attention requires identity token-position IDs and internal offsets",
                ));
            }
        }
        drop(tracker);
        Ok(index)
    }

    fn validate_count(&self) -> OperationResult<()> {
        let ids = self.id_tracker.borrow();
        let storage = self.vector_storage.borrow();
        if ids.available_point_count() != self.head.n
            || ids.total_point_count() != self.head.n
            || storage.available_vector_count() != self.head.n
            || storage.deleted_vector_count() != 0
        {
            return Err(OperationError::validation_error(
                "page_attention generation must cover the entire immutable segment, without updates or deletes",
            ));
        }
        Ok(())
    }

    pub fn heap_bytes(&self) -> u64 {
        // Side arrays are expanded to f32 on load; codes alone stay mapped.
        (self.head.logical.len() * 20
            + self.head.inverse.len() * 4
            + self.head.groups.n_groups * (8 * self.head.dim + 4)
            + 3 * self.head.dim * 4) as u64
            + self.head.graph.bytes()
    }
}

impl VectorIndexRead for PageAttentionIndex {
    fn search(
        &self,
        vectors: &[&QueryVector],
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
        context: &VectorQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        self.validate_count()?;
        let dim = self.head.dim;
        let stopped = context.is_stopped();
        check_process_stopped(&stopped)?;
        let ef = params.and_then(|p| p.hnsw_ef).unwrap_or(16);
        if filter.is_some() || top < dim + 1 || ef == 0 || params.is_some_and(|p| p.exact) {
            return Err(OperationError::validation_error(
                "page_attention requires no filter, limit >= head_dim + 1, ef > 0 and exact=false",
            ));
        }
        vectors
            .par_iter()
            .map(|query| {
                check_process_stopped(&stopped)?;
                let QueryVector::Nearest(VectorInternal::Dense(q)) = query else {
                    return Err(OperationError::validation_error(
                        "page_attention supports dense nearest queries only",
                    ));
                };
                if q.len() != dim * 2
                    || q[dim..].iter().any(|&x| x != 0.0)
                    || q.iter().any(|x| !x.is_finite())
                {
                    return Err(OperationError::validation_error(
                        "page_attention expects a finite query padded with head_dim zeroes",
                    ));
                }
                let storage = self.vector_storage.borrow();
                let mut originals = Originals {
                    storage: &storage,
                    query: &q[..dim],
                };
                let rescore = if params
                    .and_then(|p| p.quantization.as_ref())
                    .and_then(|p| p.rescore)
                    == Some(false)
                {
                    0
                } else {
                    self.config.rescore
                };
                let attention = pagesearch::attend(
                    &self.head,
                    &self.rotation,
                    &q[..dim],
                    1.0 / (dim as f32).sqrt(),
                    0,
                    ef,
                    0,
                    true,
                    rescore,
                    Some(&mut originals),
                    Parameters::default(),
                );
                let mut result: Vec<_> = attention
                    .out
                    .into_iter()
                    .chain([attention.lse])
                    .enumerate()
                    .map(|(idx, score)| ScoredPointOffset {
                        idx: idx as u32,
                        score,
                    })
                    .collect();
                if result.iter().any(|p| !p.score.is_finite()) {
                    return Err(OperationError::validation_error(
                        "page_attention produced a non-finite output",
                    ));
                }
                // Shard merging assumes descending scores, even though the client sorts by id.
                result.sort_unstable_by(|a, b| b.score.total_cmp(&a.score));
                Ok(result)
            })
            .collect()
    }
    fn get_telemetry_data(&self, _: TelemetryDetail) -> VectorIndexSearchesTelemetry {
        Default::default()
    }
    fn indexed_vector_count(&self) -> usize {
        self.head.n
    }
    fn size_of_searchable_vectors_in_bytes(&self) -> usize {
        self.head.kv_bytes() as usize
    }
    fn fill_idf_statistics(
        &self,
        _: &mut HashMap<DimId, usize>,
        _: Option<&Filter>,
        _: &AtomicBool,
        _: &HardwareCounterCell,
    ) -> OperationResult<usize> {
        Ok(0)
    }
    fn is_index(&self) -> bool {
        true
    }
}

impl VectorIndex for PageAttentionIndex {
    fn files(&self) -> Vec<PathBuf> {
        self.files.clone()
    }
    fn immutable_files(&self) -> Vec<PathBuf> {
        self.files.clone()
    }
    fn update_vector(
        &mut self,
        _: PointOffsetType,
        _: Option<VectorRef>,
        _: &HardwareCounterCell,
    ) -> OperationResult<()> {
        Err(OperationError::validation_error(
            "page_attention is immutable; build a new collection",
        ))
    }
    fn update_vector_raw(
        &mut self,
        _: PointOffsetType,
        _: Option<&[u8]>,
        _: &HardwareCounterCell,
    ) -> OperationResult<()> {
        Err(OperationError::validation_error(
            "page_attention is immutable; build a new collection",
        ))
    }
}
