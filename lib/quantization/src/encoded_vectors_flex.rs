use std::alloc::Layout;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use common::counter::hardware_counter::HardwareCounterCell;
use common::typelevel::True;
use common::types::PointOffsetType;
use fs_err as fs;
use io::file_operations::atomic_save_json;
use memory::mmap_type::MmapFlusher;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

use crate::EncodingError;
use crate::encoded_storage::{EncodedStorage, EncodedStorageBuilder};
use crate::encoded_vectors::{EncodedVectors, VectorParameters};
use crate::vector_stats::{VectorElementStats, VectorStats};

pub const ALIGNMENT: usize = 8;

pub struct EncodedVectorsFlex<TStorage: EncodedStorage> {
    encoded_vectors: TStorage,
    metadata: Metadata,
    metadata_path: Option<PathBuf>,
    queries: Vec<EncodedQueryFlex>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct EncodedQueryFlex {
    offset: f32,
    original: Vec<f32>,
    transformed: Vec<f32>,
}

#[derive(Serialize, Deserialize)]
struct Metadata {
    actual_dim: usize,
    bits_count: usize,
    vector_parameters: VectorParameters,
    vector_stats: VectorStats,
    transform: Transform,
}

impl<TStorage: EncodedStorage> EncodedVectorsFlex<TStorage> {
    pub fn storage(&self) -> &TStorage {
        &self.encoded_vectors
    }

    pub fn encode<'a>(
        orig_data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone,
        mut storage_builder: impl EncodedStorageBuilder<Storage = TStorage>,
        vector_parameters: &VectorParameters,
        count: usize,
        quantile: Option<f32>,
        rotations: Option<usize>,
        sigmas: Option<f32>,
        bits_count: usize,
        meta_path: Option<&Path>,
        stopped: &AtomicBool,
    ) -> Result<Self, EncodingError> {
        let actual_dim = Self::get_actual_dim(vector_parameters);

        if count == 0 {
            unimplemented!("Encoding zero vectors is not supported yet");
        }

        let vector_stats = VectorStats::build(orig_data.clone(), vector_parameters);
        let transform = Transform::new(
            orig_data.clone(),
            &vector_stats,
            vector_parameters,
            sigmas.unwrap_or(3.0),
            rotations.unwrap_or(0),
            meta_path,
        );

        let metadata = Metadata {
            actual_dim,
            vector_parameters: vector_parameters.clone(),
            vector_stats,
            transform,
            bits_count,
        };

        let mut queries = Vec::with_capacity(count);
        for vector in orig_data {
            if stopped.load(Ordering::Relaxed) {
                return Err(EncodingError::Stopped);
            }

            let encoded_query = Self::encode_query(&metadata, vector.as_ref());
            queries.push(encoded_query);

            let mut encoded_vector = Vec::with_capacity(actual_dim + std::mem::size_of::<f32>());
            Self::encode_vector(&metadata, vector.as_ref(), &mut encoded_vector);

            storage_builder
                .push_vector_data(&encoded_vector)
                .map_err(|e| {
                    EncodingError::EncodingError(format!("Failed to push encoded vector: {e}",))
                })?;
        }

        let encoded_vectors = storage_builder
            .build()
            .map_err(|e| EncodingError::EncodingError(format!("Failed to build storage: {e}",)))?;

        if let Some(meta_path) = meta_path {
            let bin_path = meta_path.with_extension("orig");
            let file = std::fs::File::create(bin_path).map_err(|e| {
                EncodingError::EncodingError(format!("Failed to create original data file: {e}",))
            })?;
            bincode::serialize_into(std::io::BufWriter::new(file), &queries).map_err(|e| {
                EncodingError::EncodingError(format!("Failed to save original data: {e}",))
            })?;

            meta_path
                .parent()
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Path must have a parent directory",
                    )
                })
                .and_then(fs::create_dir_all)
                .map_err(|e| {
                    EncodingError::EncodingError(format!(
                        "Failed to create metadata directory: {e}",
                    ))
                })?;
            atomic_save_json(meta_path, &metadata).map_err(|e| {
                EncodingError::EncodingError(format!("Failed to save metadata: {e}",))
            })?;
        }

        Ok(EncodedVectorsFlex {
            encoded_vectors,
            metadata,
            metadata_path: meta_path.map(PathBuf::from),
            queries,
        })
    }

    pub fn load(encoded_vectors: TStorage, meta_path: &Path) -> std::io::Result<Self> {
        let contents = fs::read_to_string(meta_path)?;
        let metadata: Metadata = serde_json::from_str(&contents)?;

        let queries_data_path = meta_path.with_extension("qbin");
        let file = std::fs::File::open(queries_data_path)?;
        let queries: Vec<EncodedQueryFlex> =
            bincode::deserialize_from(std::io::BufReader::new(file)).unwrap();

        let result = Self {
            encoded_vectors,
            metadata,
            metadata_path: Some(meta_path.to_path_buf()),
            queries,
        };
        Ok(result)
    }

    pub fn get_quantized_vector(&self, i: PointOffsetType) -> &[u8] {
        self.encoded_vectors.get_vector_data(i)
    }

    pub fn layout(&self) -> Layout {
        Layout::from_size_align(self.quantized_vector_size(), align_of::<u8>()).unwrap()
    }

    pub fn get_quantized_vector_size(vector_parameters: &VectorParameters) -> usize {
        let actual_dim = Self::get_actual_dim(vector_parameters);
        actual_dim + std::mem::size_of::<f32>()
    }

    pub fn get_actual_dim(vector_parameters: &VectorParameters) -> usize {
        vector_parameters.dim + (ALIGNMENT - vector_parameters.dim % ALIGNMENT) % ALIGNMENT
    }

    fn encode_query(metadata: &Metadata, query: &[f32]) -> EncodedQueryFlex {
        let mut query = query.to_owned();
        while query.len() % ALIGNMENT != 0 {
            query.push(0.0);
        }
        let (transformed_query, offset) = metadata.transform.transform_query(&query);
        EncodedQueryFlex {
            offset,
            original: query,
            transformed: transformed_query,
        }
    }

    fn encode_vector(metadata: &Metadata, vector: &[f32], result: &mut [u8]) {
        result.fill(0);
        let (mut transformed_vector, vector_offset) = metadata.transform.transform_vector(vector);
        result[0..std::mem::size_of::<f32>()].copy_from_slice(&vector_offset.to_ne_bytes());
        while transformed_vector.len() % ALIGNMENT != 0 {
            transformed_vector.push(0.0);
        }
        let result = &mut result[std::mem::size_of::<f32>()..];
        let mut intbuf = [0i32; ALIGNMENT];
        for (chunk_index, chunk) in transformed_vector.chunks_exact(ALIGNMENT).enumerate() {
            for (i, &value) in chunk.iter().enumerate() {
                intbuf[i] = ((2 << 10) as f32 * value) as i32;
            }

            todo!()
        }
    }
}

impl<TStorage: EncodedStorage> EncodedVectors for EncodedVectorsFlex<TStorage> {
    type EncodedQuery = EncodedQueryFlex;

    fn is_on_disk(&self) -> bool {
        self.encoded_vectors.is_on_disk()
    }

    fn encode_query(&self, query: &[f32]) -> EncodedQueryFlex {
        Self::encode_query(&self.metadata, query)
    }

    fn score_point(
        &self,
        query: &EncodedQueryFlex,
        i: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> f32 {
        let bytes = self.encoded_vectors.get_vector_data(i);
        self.score_bytes(True, query, bytes, hw_counter)
    }

    fn score_internal(
        &self,
        i: PointOffsetType,
        j: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> f32 {
        hw_counter
            .cpu_counter()
            .incr_delta(self.metadata.vector_parameters.dim);

        hw_counter
            .vector_io_read()
            .incr_delta(self.metadata.vector_parameters.dim * 2);

        self.score_bytes(
            True,
            &self.queries[i as usize],
            self.encoded_vectors.get_vector_data(j),
            hw_counter,
        )
    }

    fn quantized_vector_size(&self) -> usize {
        // actual_dim rounds up vector_dimension to the next multiple of ALIGNMENT
        // also add scaling factor to the tally
        self.metadata.actual_dim + std::mem::size_of::<f32>()
    }

    fn encode_internal_vector(&self, id: PointOffsetType) -> Option<EncodedQueryFlex> {
        Some(self.queries[id as usize].clone())
    }

    fn upsert_vector(
        &mut self,
        _id: PointOffsetType,
        _vector: &[f32],
        _hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        debug_assert!(false, "SQ does not support upsert_vector",);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "SQ does not support upsert_vector",
        ))
    }

    fn vectors_count(&self) -> usize {
        self.queries.len()
    }

    fn flusher(&self) -> MmapFlusher {
        self.encoded_vectors.flusher()
    }

    fn files(&self) -> Vec<PathBuf> {
        let mut files = self.encoded_vectors.files();
        if let Some(meta_path) = &self.metadata_path {
            files.push(meta_path.clone());
        }
        files
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        let mut files = self.encoded_vectors.immutable_files();
        if let Some(meta_path) = &self.metadata_path {
            files.push(meta_path.clone());
        }
        files
    }

    type SupportsBytes = True;
    fn score_bytes(
        &self,
        _: Self::SupportsBytes,
        query: &Self::EncodedQuery,
        bytes: &[u8],
        hw_counter: &HardwareCounterCell,
    ) -> f32 {
        hw_counter
            .cpu_counter()
            .incr_delta(self.metadata.vector_parameters.dim);

        todo!()
    }
}

#[derive(Serialize, Deserialize)]
pub struct Transform {
    rotation: Option<Rotation>,
    shifter: Shifter,
    stddevs: Vec<f32>,
    mean_sqr_sum: f32,
    sigmas: f32,
}

impl Transform {
    pub fn new(
        data: impl Iterator<Item = impl AsRef<[f32]>> + Clone,
        vector_stats: &VectorStats,
        vector_params: &VectorParameters,
        sigmas: f32,
        rotation_steps: usize,
        debug_path: Option<&Path>,
    ) -> Self {
        let mean_sqr_sum = vector_stats
            .elements_stats
            .iter()
            .map(|m| (m.mean * m.mean) as f32)
            .sum();
        let stddevs = vector_stats
            .elements_stats
            .iter()
            .map(|element_stats| {
                let VectorElementStats {
                    min: _,
                    max: _,
                    mean: _,
                    stddev,
                } = element_stats;
                *stddev
            })
            .collect::<Vec<f32>>();

        let shifter = Shifter::new(vector_stats);
        if rotation_steps > 0 {
            let rotation = Rotation::new(
                data.map(|v| {
                    let mut vector = v.as_ref().to_vec();
                    shifter.shift(&mut vector);
                    vector
                }),
                vector_params,
                rotation_steps,
                debug_path,
            );
            Self {
                rotation: Some(rotation),
                shifter,
                stddevs,
                mean_sqr_sum,
                sigmas,
            }
        } else {
            Self {
                rotation: None,
                shifter,
                stddevs,
                mean_sqr_sum,
                sigmas,
            }
        }
    }

    pub fn transform_query(&self, query: &[f32]) -> (Vec<f32>, f32) {
        let mut vector = query.to_owned();
        let sum = self.shifter.shift(&mut vector);
        for (v, stddev) in vector.iter_mut().zip(self.stddevs.iter()) {
            if *stddev > f32::EPSILON {
                *v *= self.sigmas * *stddev;
            } else {
                *v = 0.0;
            }
        }
        if let Some(rotation) = &self.rotation {
            rotation.rotate(&mut vector);
        }
        (vector, sum)
    }

    pub fn transform_vector(&self, vector: &[f32]) -> (Vec<f32>, f32) {
        let mut vector = vector.to_owned();
        let sum = self.shifter.shift(&mut vector);
        for (v, stddev) in vector.iter_mut().zip(self.stddevs.iter()) {
            if *stddev > f32::EPSILON {
                *v /= self.sigmas * *stddev;
                *v = v.clamp(-1.0, 1.0);
            } else {
                *v = 0.0;
            }
        }
        if let Some(rotation) = &self.rotation {
            rotation.rotate(&mut vector);
        }
        (vector, sum)
    }
}

#[derive(Serialize, Deserialize)]
pub struct Shifter {
    vector_stats: Option<VectorStats>,
}

impl Shifter {
    pub fn new(vector_stats: &VectorStats) -> Self {
        let skip = std::env::var("SHIFTING")
            .unwrap_or_default()
            .trim()
            .parse()
            .unwrap_or(1)
            == 0;

        if skip {
            log::info!("Skipping shifting as per environment variable");
        }

        Self {
            vector_stats: Some(vector_stats.clone()),
        }
    }

    pub fn shift(&self, vector: &mut [f32]) -> f32 {
        if let Some(vector_stats) = &self.vector_stats {
            for (v, stats) in vector.iter_mut().zip(vector_stats.elements_stats.iter()) {
                *v -= stats.mean;
            }
            vector_stats
                .elements_stats
                .iter()
                .zip(vector.iter())
                .map(|(s, &v)| v * s.mean)
                .sum()
        } else {
            0.0
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Rotation {
    simple_rotations: Vec<SimpleRotation>,
}

#[derive(Serialize, Deserialize)]
pub struct SimpleRotation {
    permutation: Vec<usize>,
    pair_rotations: usize,
    vector_stats: VectorStats,
}

impl Rotation {
    pub fn new(
        data: impl Iterator<Item = impl AsRef<[f32]>> + Clone,
        vector_params: &VectorParameters,
        steps: usize,
        debug_path: Option<&Path>,
    ) -> Self {
        if data.clone().next().is_none() {
            return Self {
                simple_rotations: vec![],
            };
        }

        let dump = std::env::var("DUMP_IMAGES").unwrap_or_default();
        if dump == "1" {
            if let Some(debug_path) = debug_path {
                std::fs::create_dir_all(debug_path).ok();
                let debug_path = debug_path.join(format!("orig_histograms"));
                std::fs::create_dir_all(&debug_path).ok();
                for dim in 0..vector_params.dim {
                    let numbers = data.clone().map(|v| v.as_ref()[dim]).collect::<Vec<f32>>();
                    plot_histogram(
                        &numbers,
                        &debug_path.join(format!("orig_histogram_{dim}.png")),
                        None,
                    )
                    .unwrap();
                }
            }
        }

        log::info!("Rotation steps: {steps}");

        let mut simple_rotations: Vec<SimpleRotation> = vec![];
        for step in 0..steps {
            let data = data.clone().map(|v| {
                let mut vector = v.as_ref().to_vec();
                for simple_rotation in &simple_rotations {
                    simple_rotation.rotate(&mut vector);
                }
                vector
            });
            let debug_path = debug_path.map(|p| p.join(format!("rotation_{step}")));
            let rotation = SimpleRotation::new(
                data.clone(),
                vector_params,
                debug_path.as_ref().map(|p| p.as_ref()),
            );
            simple_rotations.push(rotation);
        }

        Self { simple_rotations }
    }

    pub fn rotate(&self, vector: &mut [f32]) {
        for simple_rotation in &self.simple_rotations {
            simple_rotation.rotate(vector);
        }
    }
}

impl SimpleRotation {
    fn new(
        data: impl Iterator<Item = impl AsRef<[f32]>> + Clone,
        vector_params: &VectorParameters,
        debug_path: Option<&Path>,
    ) -> Self {
        if let Some(debug_path) = debug_path {
            std::fs::create_dir_all(debug_path).ok();
            for dim in 0..vector_params.dim {
                let numbers = data.clone().map(|v| v.as_ref()[dim]).collect::<Vec<f32>>();
                plot_histogram(
                    &numbers,
                    &debug_path.join(format!("histogram_{dim}.png")),
                    None,
                )
                .unwrap();
            }
        }

        let rotation_bound: f32 = std::env::var("ROTATION_BOUND")
            .unwrap_or_default()
            .trim()
            .parse()
            .unwrap_or(0.8);

        let vector_stats = VectorStats::build(data, vector_params);
        let mut indices: Vec<usize> = (0..vector_params.dim).collect();
        indices.sort_by_key(|&i| OrderedFloat(param(&vector_stats.elements_stats[i])));

        let mut permutation: Vec<usize> = Default::default();
        for i in 0..(vector_params.dim / 2) {
            permutation.push(indices[i]);
            permutation.push(indices[vector_params.dim - 1 - i]);
        }
        if vector_params.dim % 2 == 1 {
            permutation.push(indices[vector_params.dim / 2]);
        }

        let pair_rotations = permutation
            .chunks_exact(2)
            .map(|pair| (pair[0], pair[1]))
            .map(|(i, j)| {
                let param_i = param(&vector_stats.elements_stats[i]);
                let param_j = param(&vector_stats.elements_stats[j]);
                param_i.abs() / param_j.abs() < rotation_bound
            })
            .take_while(|&x| x)
            .count();

        println!(
            "Pair rotations: {pair_rotations} / {} with bound {rotation_bound}",
            vector_params.dim / 2
        );

        Self {
            permutation,
            pair_rotations,
            vector_stats,
        }
    }

    fn rotate(&self, vector: &mut [f32]) {
        // apply permutation
        let permuted = vector.to_owned();

        for (i, &p) in self.permutation.iter().enumerate() {
            vector[i] = permuted[p];
        }

        // apply 2x2 rotations
        for v in vector.chunks_exact_mut(2).take(self.pair_rotations) {
            let a = v[0] as f64;
            let b = v[1] as f64;
            let new_a = 0.707106 * a - 0.707106 * b;
            let new_b = 0.707106 * a + 0.707106 * b;
            v[0] = new_a as f32;
            v[1] = new_b as f32;
        }
    }
}

fn param(s: &VectorElementStats) -> f32 {
    s.stddev
}

pub fn plot_histogram(
    data: &[f32],
    path: &Path,
    maybe_bins: Option<usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    use plotters::prelude::*;

    let dump = std::env::var("DUMP_IMAGES").unwrap_or_default();
    if dump != "1" && dump.to_lowercase() != "true" {
        return Ok(());
    }

    let width_px: u32 = std::env::var("WIDTH_PX")
        .unwrap_or_default()
        .trim()
        .parse()
        .unwrap_or(512);

    let height_px: u32 = std::env::var("HEIGHT_PX")
        .unwrap_or_default()
        .trim()
        .parse()
        .unwrap_or(512);

    if data.is_empty() {
        return Err("Empty data array".into());
    }

    let (min_v_f32, max_v_f32) = data
        .iter()
        .fold((f32::INFINITY, f32::NEG_INFINITY), |(mn, mx), &v| {
            (mn.min(v), mx.max(v))
        });

    let (min_v, max_v) = if (max_v_f32 - min_v_f32).abs() < f32::EPSILON {
        let delta = 0.5_f32.max(max_v_f32.abs() * 0.1);
        ((min_v_f32 - delta) as f64, (max_v_f32 + delta) as f64)
    } else {
        (min_v_f32 as f64, max_v_f32 as f64)
    };

    let n = data.len();
    let bins = maybe_bins.unwrap_or_else(|| (n as f64).sqrt().round().max(5.0) as usize);

    let range = max_v - min_v;
    let bin_w = range / bins as f64;

    let safe_bin_w = if bin_w > 0.0 { bin_w } else { 1.0 };

    let mut counts = vec![0usize; bins];
    for &v in data {
        let mut idx = ((v as f64 - min_v) / safe_bin_w).floor() as isize;
        if idx < 0 {
            idx = 0;
        } else if idx as usize >= bins {
            idx = bins as isize - 1;
        }
        counts[idx as usize] += 1;
    }

    let max_count = counts.iter().copied().max().unwrap_or(1);

    let root = BitMapBackend::new(path, (width_px, height_px)).into_drawing_area();
    root.fill(&WHITE)?;

    let mut chart = ChartBuilder::on(&root)
        .margin(20)
        .caption("Histogram", ("sans-serif", 24))
        .x_label_area_size(40)
        .y_label_area_size(50)
        .build_cartesian_2d(min_v..max_v, 0..(max_count + (max_count / 10).max(1)))?;

    chart
        .configure_mesh()
        .x_desc("Value")
        .y_desc("Count")
        .disable_mesh()
        .x_labels(10)
        .y_labels(10)
        .label_style(("sans-serif", 14))
        .draw()?;

    for (i, &c) in counts.iter().enumerate() {
        let x0 = min_v + i as f64 * safe_bin_w;
        let x1 = x0 + safe_bin_w;
        let y0 = 0;
        let y1 = c;

        chart
            .draw_series(std::iter::once(Rectangle::new(
                [(x0, y0), (x1, y1)],
                BLUE.mix(0.7).filled(),
            )))?
            .label("bin")
            .legend(|(x, y)| Rectangle::new([(x, y - 5), (x + 10, y + 5)], BLUE.filled()));
    }

    chart
        .configure_series_labels()
        .border_style(&BLACK)
        .background_style(&WHITE.mix(0.8))
        .draw()?;

    root.present()?;
    Ok(())
}
