mod gpu_multivectors;
mod gpu_quantization;

#[cfg(test)]
mod tests;

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use common::types::PointOffsetType;
use gpu_multivectors::GpuMultivectors;
use gpu_quantization::GpuQuantization;
use quantization::encoded_vectors_binary::{BitsStoreType, EncodedVectorsBin};
use quantization::{EncodedStorage, EncodedVectorsPQ, EncodedVectorsU8};

use super::shader_builder::ShaderBuilderParameters;
use crate::common::operation_error::{check_process_stopped, OperationError, OperationResult};
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{VectorElementType, VectorElementTypeByte, VectorElementTypeHalf};
use crate::index::hnsw_index::gpu::GPU_TIMEOUT;
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::quantized::quantized_vectors::{
    QuantizedVectorStorage, QuantizedVectors,
};
use crate::vector_storage::{
    DenseVectorStorage, MultiVectorStorage, VectorStorage, VectorStorageEnum,
};

pub const ELEMENTS_PER_SUBGROUP: usize = 4;
pub const UPLOAD_CHUNK_SIZE: usize = 64 * 1024 * 1024;
pub const STORAGES_COUNT: usize = 4;

/// GPU storage for vectors.
pub struct GpuVectorStorage {
    device: Arc<gpu::Device>,
    num_vectors: usize,
    descriptor_set_layout: Arc<gpu::DescriptorSetLayout>,
    descriptor_set: Arc<gpu::DescriptorSet>,
    dim: usize,
    element_type: VectorStorageDatatype,
    distance: Distance,
    /// Additional quantization data.
    quantization: Option<GpuQuantization>,
    /// Additional multivectors data.
    multivectors: Option<GpuMultivectors>,
}

impl ShaderBuilderParameters for GpuVectorStorage {
    fn shader_includes(&self) -> HashMap<String, String> {
        let mut includes = HashMap::from([
            (
                "vector_storage.comp".to_string(),
                include_str!("../shaders/vector_storage.comp").to_string(),
            ),
            (
                "vector_storage_dense.comp".to_string(),
                include_str!("../shaders/vector_storage_dense.comp").to_string(),
            ),
            (
                "vector_storage_f16.comp".to_string(),
                include_str!("../shaders/vector_storage_f16.comp").to_string(),
            ),
            (
                "vector_storage_f32.comp".to_string(),
                include_str!("../shaders/vector_storage_f32.comp").to_string(),
            ),
            (
                "vector_storage_u8.comp".to_string(),
                include_str!("../shaders/vector_storage_u8.comp").to_string(),
            ),
        ]);

        if let Some(quantization) = &self.quantization {
            includes.extend(quantization.shader_includes());
        }
        if let Some(multivectors) = &self.multivectors {
            includes.extend(multivectors.shader_includes());
        }

        includes
    }

    fn shader_defines(&self) -> HashMap<String, Option<String>> {
        let mut defines = HashMap::new();
        match self.element_type {
            VectorStorageDatatype::Float32 => {
                defines.insert("VECTOR_STORAGE_ELEMENT_FLOAT32".to_owned(), None);
            }
            VectorStorageDatatype::Float16 => {
                defines.insert("VECTOR_STORAGE_ELEMENT_FLOAT16".to_owned(), None);
            }
            VectorStorageDatatype::Uint8 => {
                defines.insert("VECTOR_STORAGE_ELEMENT_UINT8".to_owned(), None);
            }
        }

        match self.distance {
            Distance::Cosine => {
                defines.insert("COSINE_DISTANCE".to_owned(), None);
            }
            Distance::Euclid => {
                defines.insert("EUCLID_DISTANCE".to_owned(), None);
            }
            Distance::Dot => {
                defines.insert("DOT_DISTANCE".to_owned(), None);
            }
            Distance::Manhattan => {
                defines.insert("MANHATTAN_DISTANCE".to_owned(), None);
            }
        }

        if let Some(quantization) = &self.quantization {
            defines.extend(quantization.shader_defines());
        }

        if let Some(multivectors) = &self.multivectors {
            defines.extend(multivectors.shader_defines());
        }

        defines.insert("DIM".to_owned(), Some(self.dim.to_string()));
        defines.insert(
            "STORAGES_COUNT".to_owned(),
            Some(STORAGES_COUNT.to_string()),
        );
        defines
    }
}

impl GpuVectorStorage {
    pub fn new(
        device: Arc<gpu::Device>,
        vector_storage: &VectorStorageEnum,
        quantized_storage: Option<&QuantizedVectors>,
        // Force half precision for `f32` vectors.
        force_half_precision: bool,
        stopped: &AtomicBool,
    ) -> OperationResult<Self> {
        // GPU buffers should not be empty.
        // Check that we have enough vectors to store at least one vector in each buffer.
        if vector_storage.total_vector_count() < STORAGES_COUNT {
            return Err(OperationError::service_error(
                "Vectors count is less than `STORAGES_COUNT`",
            ));
        }
        if let Some(quantized_storage) = quantized_storage {
            Self::new_quantized(
                device,
                vector_storage.distance(),
                quantized_storage.get_storage(),
                stopped,
            )
        } else {
            Self::new_from_vector_storage(device, vector_storage, force_half_precision, stopped)
        }
    }

    fn new_quantized(
        device: Arc<gpu::Device>,
        distance: Distance,
        quantized_storage: &QuantizedVectorStorage,
        stopped: &AtomicBool,
    ) -> OperationResult<Self> {
        match quantized_storage {
            QuantizedVectorStorage::ScalarRam(quantized_storage) => Self::new_sq(
                device.clone(),
                distance,
                quantized_storage.vectors_count(),
                quantized_storage,
                None,
                stopped,
            ),
            QuantizedVectorStorage::ScalarMmap(quantized_storage) => Self::new_sq(
                device.clone(),
                distance,
                quantized_storage.vectors_count(),
                quantized_storage,
                None,
                stopped,
            ),
            QuantizedVectorStorage::PQRam(quantized_storage) => Self::new_pq(
                device.clone(),
                distance,
                quantized_storage.vectors_count(),
                quantized_storage,
                None,
                stopped,
            ),
            QuantizedVectorStorage::PQMmap(quantized_storage) => Self::new_pq(
                device.clone(),
                distance,
                quantized_storage.vectors_count(),
                quantized_storage,
                None,
                stopped,
            ),
            QuantizedVectorStorage::BinaryRam(quantized_storage) => Self::new_bq(
                device.clone(),
                distance,
                quantized_storage.vectors_count(),
                quantized_storage,
                None,
                stopped,
            ),
            QuantizedVectorStorage::BinaryMmap(quantized_storage) => Self::new_bq(
                device.clone(),
                distance,
                quantized_storage.vectors_count(),
                quantized_storage,
                None,
                stopped,
            ),
            QuantizedVectorStorage::ScalarRamMulti(quantized_storage) => Self::new_sq(
                device.clone(),
                distance,
                quantized_storage.vectors_count(),
                quantized_storage.inner_storage(),
                Some(GpuMultivectors::new_quantized(device, quantized_storage)?),
                stopped,
            ),
            QuantizedVectorStorage::ScalarMmapMulti(quantized_storage) => Self::new_sq(
                device.clone(),
                distance,
                quantized_storage.vectors_count(),
                quantized_storage.inner_storage(),
                Some(GpuMultivectors::new_quantized(device, quantized_storage)?),
                stopped,
            ),
            QuantizedVectorStorage::PQRamMulti(quantized_storage) => Self::new_pq(
                device.clone(),
                distance,
                quantized_storage.vectors_count(),
                quantized_storage.inner_storage(),
                Some(GpuMultivectors::new_quantized(device, quantized_storage)?),
                stopped,
            ),
            QuantizedVectorStorage::PQMmapMulti(quantized_storage) => Self::new_pq(
                device.clone(),
                distance,
                quantized_storage.vectors_count(),
                quantized_storage.inner_storage(),
                Some(GpuMultivectors::new_quantized(device, quantized_storage)?),
                stopped,
            ),
            QuantizedVectorStorage::BinaryRamMulti(quantized_storage) => Self::new_bq(
                device.clone(),
                distance,
                quantized_storage.vectors_count(),
                quantized_storage.inner_storage(),
                Some(GpuMultivectors::new_quantized(device, quantized_storage)?),
                stopped,
            ),
            QuantizedVectorStorage::BinaryMmapMulti(quantized_storage) => Self::new_bq(
                device.clone(),
                distance,
                quantized_storage.vectors_count(),
                quantized_storage.inner_storage(),
                Some(GpuMultivectors::new_quantized(device, quantized_storage)?),
                stopped,
            ),
        }
    }

    pub fn new_sq<TStorage: EncodedStorage>(
        device: Arc<gpu::Device>,
        distance: Distance,
        num_vectors: usize,
        quantized_storage: &EncodedVectorsU8<TStorage>,
        multivectors: Option<GpuMultivectors>,
        stopped: &AtomicBool,
    ) -> OperationResult<Self> {
        Self::new_typed::<VectorElementTypeByte>(
            device.clone(),
            distance,
            quantized_storage.vectors_count(),
            num_vectors,
            quantized_storage.get_quantized_vector(0).1.len(),
            (0..quantized_storage.vectors_count()).map(|id| {
                let (_, vector) = quantized_storage.get_quantized_vector(id as PointOffsetType);
                Cow::Borrowed(vector)
            }),
            Some(GpuQuantization::new_sq(device, quantized_storage)?),
            multivectors,
            stopped,
        )
    }

    fn new_pq<TStorage: EncodedStorage>(
        device: Arc<gpu::Device>,
        distance: Distance,
        num_vectors: usize,
        quantized_storage: &EncodedVectorsPQ<TStorage>,
        multivectors: Option<GpuMultivectors>,
        stopped: &AtomicBool,
    ) -> OperationResult<Self> {
        Self::new_typed::<VectorElementTypeByte>(
            device.clone(),
            distance,
            quantized_storage.vectors_count(),
            num_vectors,
            quantized_storage.get_quantized_vector(0).len(),
            (0..quantized_storage.vectors_count()).map(|id| {
                let vector = quantized_storage.get_quantized_vector(id as PointOffsetType);
                Cow::Borrowed(vector)
            }),
            Some(GpuQuantization::new_pq(device, quantized_storage)?),
            multivectors,
            stopped,
        )
    }

    fn new_bq<T: BitsStoreType, TStorage: EncodedStorage>(
        device: Arc<gpu::Device>,
        distance: Distance,
        num_vectors: usize,
        quantized_storage: &EncodedVectorsBin<T, TStorage>,
        multivectors: Option<GpuMultivectors>,
        stopped: &AtomicBool,
    ) -> OperationResult<Self> {
        Self::new_typed::<VectorElementTypeByte>(
            device.clone(),
            distance,
            quantized_storage.vectors_count(),
            num_vectors,
            quantized_storage.get_quantized_vector(0).len(),
            (0..quantized_storage.vectors_count()).map(|id| {
                Cow::Borrowed(quantized_storage.get_quantized_vector(id as PointOffsetType))
            }),
            Some(GpuQuantization::new_bq(device, quantized_storage)),
            multivectors,
            stopped,
        )
    }

    fn new_from_vector_storage(
        device: Arc<gpu::Device>,
        vector_storage: &VectorStorageEnum,
        force_half_precision: bool,
        stopped: &AtomicBool,
    ) -> OperationResult<Self> {
        match vector_storage {
            VectorStorageEnum::DenseSimple(vector_storage) => {
                Self::new_dense_f32(device, vector_storage, force_half_precision, stopped)
            }
            VectorStorageEnum::DenseSimpleByte(vector_storage) => {
                Self::new_dense(device, vector_storage, stopped)
            }
            VectorStorageEnum::DenseSimpleHalf(vector_storage) => {
                Self::new_dense(device, vector_storage, stopped)
            }
            VectorStorageEnum::DenseMemmap(vector_storage) => Self::new_dense_f32(
                device,
                vector_storage.as_ref(),
                force_half_precision,
                stopped,
            ),
            VectorStorageEnum::DenseMemmapByte(vector_storage) => {
                Self::new_dense(device, vector_storage.as_ref(), stopped)
            }
            VectorStorageEnum::DenseMemmapHalf(vector_storage) => {
                Self::new_dense(device, vector_storage.as_ref(), stopped)
            }
            VectorStorageEnum::DenseAppendableMemmap(vector_storage) => Self::new_dense_f32(
                device,
                vector_storage.as_ref(),
                force_half_precision,
                stopped,
            ),
            VectorStorageEnum::DenseAppendableMemmapByte(vector_storage) => {
                Self::new_dense(device, vector_storage.as_ref(), stopped)
            }
            VectorStorageEnum::DenseAppendableMemmapHalf(vector_storage) => {
                Self::new_dense(device, vector_storage.as_ref(), stopped)
            }
            VectorStorageEnum::DenseAppendableInRam(vector_storage) => Self::new_dense_f32(
                device,
                vector_storage.as_ref(),
                force_half_precision,
                stopped,
            ),
            VectorStorageEnum::DenseAppendableInRamByte(vector_storage) => {
                Self::new_dense(device, vector_storage.as_ref(), stopped)
            }
            VectorStorageEnum::DenseAppendableInRamHalf(vector_storage) => {
                Self::new_dense(device, vector_storage.as_ref(), stopped)
            }
            VectorStorageEnum::SparseSimple(_) => Err(OperationError::from(
                gpu::GpuError::NotSupported("Sparse vectors are not supported on GPU".to_string()),
            )),
            VectorStorageEnum::SparseMmap(_) => Err(OperationError::from(
                gpu::GpuError::NotSupported("Sparse vectors are not supported on GPU".to_string()),
            )),
            VectorStorageEnum::MultiDenseSimple(vector_storage) => Self::new_multi_f32(
                device.clone(),
                vector_storage,
                force_half_precision,
                stopped,
            ),
            VectorStorageEnum::MultiDenseSimpleByte(vector_storage) => {
                Self::new_multi(device, vector_storage, stopped)
            }
            VectorStorageEnum::MultiDenseSimpleHalf(vector_storage) => {
                Self::new_multi(device, vector_storage, stopped)
            }
            VectorStorageEnum::MultiDenseAppendableMemmap(vector_storage) => Self::new_multi_f32(
                device.clone(),
                vector_storage.as_ref(),
                force_half_precision,
                stopped,
            ),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(vector_storage) => {
                Self::new_multi(device, vector_storage.as_ref(), stopped)
            }
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(vector_storage) => {
                Self::new_multi(device, vector_storage.as_ref(), stopped)
            }
            VectorStorageEnum::MultiDenseAppendableInRam(vector_storage) => Self::new_multi_f32(
                device.clone(),
                vector_storage.as_ref(),
                force_half_precision,
                stopped,
            ),
            VectorStorageEnum::MultiDenseAppendableInRamByte(vector_storage) => {
                Self::new_multi(device, vector_storage.as_ref(), stopped)
            }
            VectorStorageEnum::MultiDenseAppendableInRamHalf(vector_storage) => {
                Self::new_multi(device, vector_storage.as_ref(), stopped)
            }
        }
    }

    fn new_dense_f32<TVectorStorage: DenseVectorStorage<VectorElementType>>(
        device: Arc<gpu::Device>,
        vector_storage: &TVectorStorage,
        force_half_precision: bool,
        stopped: &AtomicBool,
    ) -> OperationResult<Self> {
        if force_half_precision {
            Self::new_typed::<VectorElementTypeHalf>(
                device,
                vector_storage.distance(),
                vector_storage.total_vector_count(),
                vector_storage.total_vector_count(),
                vector_storage.vector_dim(),
                (0..vector_storage.total_vector_count()).map(|id| {
                    VectorElementTypeHalf::slice_from_float_cow(Cow::Borrowed(
                        vector_storage.get_dense(id as PointOffsetType),
                    ))
                }),
                None,
                None,
                stopped,
            )
        } else {
            Self::new_dense(device, vector_storage, stopped)
        }
    }

    fn new_dense<TElement: PrimitiveVectorElement, TVectorStorage: DenseVectorStorage<TElement>>(
        device: Arc<gpu::Device>,
        vector_storage: &TVectorStorage,
        stopped: &AtomicBool,
    ) -> OperationResult<Self> {
        Self::new_typed::<TElement>(
            device,
            vector_storage.distance(),
            vector_storage.total_vector_count(),
            vector_storage.total_vector_count(),
            vector_storage.vector_dim(),
            (0..vector_storage.total_vector_count())
                .map(|id| Cow::Borrowed(vector_storage.get_dense(id as PointOffsetType))),
            None,
            None,
            stopped,
        )
    }

    fn new_multi_f32<TVectorStorage: MultiVectorStorage<VectorElementType>>(
        device: Arc<gpu::Device>,
        vector_storage: &TVectorStorage,
        force_half_precision: bool,
        stopped: &AtomicBool,
    ) -> OperationResult<Self> {
        if force_half_precision {
            Self::new_typed::<VectorElementTypeHalf>(
                device.clone(),
                vector_storage.distance(),
                (0..vector_storage.total_vector_count())
                    .map(|id| {
                        vector_storage
                            .get_multi(id as PointOffsetType)
                            .vectors_count()
                    })
                    .sum(),
                vector_storage.total_vector_count(),
                vector_storage.vector_dim(),
                vector_storage.iterate_inner_vectors().map(|vector| {
                    VectorElementTypeHalf::slice_from_float_cow(Cow::Borrowed(vector))
                }),
                None,
                Some(GpuMultivectors::new_multidense(device, vector_storage)?),
                stopped,
            )
        } else {
            Self::new_multi(device, vector_storage, stopped)
        }
    }

    fn new_multi<TElement: PrimitiveVectorElement, TVectorStorage: MultiVectorStorage<TElement>>(
        device: Arc<gpu::Device>,
        vector_storage: &TVectorStorage,
        stopped: &AtomicBool,
    ) -> OperationResult<Self> {
        Self::new_typed::<TElement>(
            device.clone(),
            vector_storage.distance(),
            (0..vector_storage.total_vector_count())
                .map(|id| {
                    vector_storage
                        .get_multi(id as PointOffsetType)
                        .vectors_count()
                })
                .sum(),
            vector_storage.total_vector_count(),
            vector_storage.vector_dim(),
            vector_storage.iterate_inner_vectors().map(Cow::Borrowed),
            None,
            Some(GpuMultivectors::new_multidense(device, vector_storage)?),
            stopped,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_typed<'a, TElement: PrimitiveVectorElement>(
        device: Arc<gpu::Device>,
        distance: Distance,
        dense_count: usize,
        num_vectors: usize,
        dim: usize,
        vectors: impl Iterator<Item = Cow<'a, [TElement]>> + Clone,
        quantization: Option<GpuQuantization>,
        multivectors: Option<GpuMultivectors>,
        stopped: &AtomicBool,
    ) -> OperationResult<Self> {
        check_process_stopped(stopped)?;
        let timer = std::time::Instant::now();

        let gpu_vector_capacity = Self::gpu_vector_capacity(&device, dim);
        let gpu_vector_size = gpu_vector_capacity * std::mem::size_of::<TElement>();
        let upload_points_count = UPLOAD_CHUNK_SIZE / gpu_vector_size;

        let points_in_storage_count = Self::points_in_storage_count(dense_count);
        let vectors_buffer: Vec<Arc<gpu::Buffer>> = (0..STORAGES_COUNT)
            .map(|_| -> gpu::GpuResult<Arc<gpu::Buffer>> {
                gpu::Buffer::new(
                    device.clone(),
                    "Vector storage buffer",
                    gpu::BufferType::Storage,
                    points_in_storage_count * gpu_vector_size,
                )
            })
            .collect::<gpu::GpuResult<Vec<_>>>()?;
        log::trace!("Storage buffer size {}", vectors_buffer[0].size());

        let mut upload_context = gpu::Context::new(device.clone())?;

        // Fill all vector storages with zeros.
        for buffer in vectors_buffer.iter() {
            upload_context.clear_buffer(buffer.clone())?;
        }
        upload_context.run()?;
        upload_context.wait_finish(GPU_TIMEOUT)?;

        let staging_buffer = gpu::Buffer::new(
            device.clone(),
            "Vector storage upload staging buffer",
            gpu::BufferType::CpuToGpu,
            upload_points_count * gpu_vector_size,
        )?;
        // fill staging buffer with zeros
        let zero_vector = vec![TElement::default(); gpu_vector_capacity];
        for i in 0..upload_points_count {
            staging_buffer.upload_slice(&zero_vector, i * gpu_vector_capacity)?;
        }
        log::trace!(
            "GPU staging buffer size {}, `upload_points_count` = {}",
            staging_buffer.size(),
            upload_points_count
        );

        // Upload vectors to GPU.
        // Upload storage-by storage and iterate over all vectors for each storage.
        for (storage_index, vector_buffer) in vectors_buffer.iter().enumerate() {
            let mut gpu_offset = 0;
            let mut upload_size = 0;
            let mut upload_points = 0;

            for vector in vectors.clone().skip(storage_index).step_by(STORAGES_COUNT) {
                check_process_stopped(stopped)?;
                staging_buffer.upload_slice(vector.as_ref(), upload_points * gpu_vector_size)?;
                upload_size += gpu_vector_size;
                upload_points += 1;

                if upload_points == upload_points_count {
                    upload_context.copy_gpu_buffer(
                        staging_buffer.clone(),
                        vector_buffer.clone(),
                        0,
                        gpu_offset,
                        upload_size,
                    )?;
                    upload_context.run()?;
                    upload_context.wait_finish(GPU_TIMEOUT)?;

                    log::trace!(
                        "Uploaded {} vectors, {} MB",
                        upload_points,
                        upload_size / 1024 / 1024,
                    );

                    gpu_offset += upload_size;
                    upload_size = 0;
                    upload_points = 0;
                }
            }
            if upload_points > 0 {
                upload_context.copy_gpu_buffer(
                    staging_buffer.clone(),
                    vectors_buffer[storage_index].clone(),
                    0,
                    gpu_offset,
                    upload_size,
                )?;
                upload_context.run()?;
                upload_context.wait_finish(GPU_TIMEOUT)?;

                log::trace!(
                    "Uploaded {} vectors, {} MB",
                    upload_points,
                    upload_size / 1024 / 1024,
                );
            }
        }

        log::trace!(
            "Upload vector data to GPU time = {:?}, vector data size {} MB, element type: {:?}",
            timer.elapsed(),
            STORAGES_COUNT * points_in_storage_count * gpu_vector_size / 1024 / 1024,
            TElement::datatype(),
        );

        let descriptor_set_layout =
            Self::create_descriptor_set_layout(device.clone(), &quantization, &multivectors)?;
        let descriptor_set = Self::create_descriptor_set(
            descriptor_set_layout.clone(),
            &vectors_buffer,
            &quantization,
            &multivectors,
        )?;

        Ok(Self {
            device,
            descriptor_set_layout,
            descriptor_set,
            dim: gpu_vector_capacity,
            num_vectors,
            element_type: TElement::datatype(),
            distance,
            quantization,
            multivectors,
        })
    }

    fn create_descriptor_set_layout(
        device: Arc<gpu::Device>,
        quantization: &Option<GpuQuantization>,
        multivectors: &Option<GpuMultivectors>,
    ) -> OperationResult<Arc<gpu::DescriptorSetLayout>> {
        let mut descriptor_set_layout_builder = gpu::DescriptorSetLayout::builder();
        for i in 0..STORAGES_COUNT {
            descriptor_set_layout_builder = descriptor_set_layout_builder.add_storage_buffer(i);
        }
        descriptor_set_layout_builder = if let Some(quantization) = &quantization {
            quantization.add_descriptor_set_layout(descriptor_set_layout_builder)
        } else {
            descriptor_set_layout_builder
        };
        descriptor_set_layout_builder = if let Some(multivectors) = &multivectors {
            multivectors.add_descriptor_set_layout(descriptor_set_layout_builder)
        } else {
            descriptor_set_layout_builder
        };
        Ok(descriptor_set_layout_builder.build(device.clone())?)
    }

    fn create_descriptor_set(
        descriptor_set_layout: Arc<gpu::DescriptorSetLayout>,
        vectors_buffer: &[Arc<gpu::Buffer>],
        quantization: &Option<GpuQuantization>,
        multivectors: &Option<GpuMultivectors>,
    ) -> OperationResult<Arc<gpu::DescriptorSet>> {
        let mut descriptor_set_builder = gpu::DescriptorSet::builder(descriptor_set_layout.clone());
        for (i, vector_buffer) in vectors_buffer.iter().enumerate() {
            descriptor_set_builder =
                descriptor_set_builder.add_storage_buffer(i, vector_buffer.clone());
        }
        descriptor_set_builder = if let Some(quantization) = &quantization {
            quantization.add_descriptor_set(descriptor_set_builder)
        } else {
            descriptor_set_builder
        };
        descriptor_set_builder = if let Some(multivectors) = &multivectors {
            multivectors.add_descriptor_set(descriptor_set_builder)
        } else {
            descriptor_set_builder
        };
        Ok(descriptor_set_builder.build()?)
    }

    /// GPU aligned vector size in elements count.
    fn gpu_vector_capacity(device: &Arc<gpu::Device>, dim: usize) -> usize {
        // BQ is a `u8` vector with `u128`=16bytes alignment.
        // We need to keep this alignment for bq case that's why we have `max` here.
        // But TBH even CPU emulator has 8 subgroups to that we don't expect in practice
        // that we will have less than 16 bytes alignment.
        let alignment = std::cmp::max(device.subgroup_size() * ELEMENTS_PER_SUBGROUP, 16);
        dim.next_multiple_of(alignment)
    }

    /// Number of vectors in each gpu buffer.
    fn points_in_storage_count(num_vectors: usize) -> usize {
        num_vectors.next_multiple_of(STORAGES_COUNT) / STORAGES_COUNT
    }

    pub fn descriptor_set_layout(&self) -> Arc<gpu::DescriptorSetLayout> {
        self.descriptor_set_layout.clone()
    }

    pub fn descriptor_set(&self) -> Arc<gpu::DescriptorSet> {
        self.descriptor_set.clone()
    }

    pub fn device(&self) -> Arc<gpu::Device> {
        self.device.clone()
    }

    pub fn num_vectors(&self) -> usize {
        self.num_vectors
    }
}
