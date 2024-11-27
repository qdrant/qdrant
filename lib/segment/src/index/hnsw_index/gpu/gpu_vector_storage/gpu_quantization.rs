use std::collections::HashMap;
use std::sync::Arc;

use common::types::PointOffsetType;
use quantization::encoded_vectors_binary::{BitsStoreType, EncodedVectorsBin};
use quantization::{EncodedStorage, EncodedVectorsPQ, EncodedVectorsU8};

use super::{GpuVectorStorage, STORAGES_COUNT};
use crate::common::operation_error::OperationResult;
use crate::index::hnsw_index::gpu::shader_builder::ShaderBuilderParameters;
use crate::index::hnsw_index::gpu::GPU_TIMEOUT;

pub const START_QUANTIZATION_BINDING: usize = STORAGES_COUNT;
pub const MAX_QUANTIZATION_BINDINGS: usize = 2;

/// Additional data for quantization in GPU.
/// Add quantized vectors are stored as reqular vectors with for instance `u8` type.
/// But quantization requires additional data for decoding.
/// For instance, centroids for product quantization.
/// This structures provides all additional data for quantization.
/// From gpu buffers to shader defines and additional shader files.
pub enum GpuQuantization {
    Binary(GpuBinaryQuantization),
    Scalar(GpuScalarQuantization),
    Product(GpuProductQuantization),
}

impl ShaderBuilderParameters for GpuQuantization {
    fn shader_includes(&self) -> HashMap<String, String> {
        match self {
            GpuQuantization::Binary(params) => params.shader_includes(),
            GpuQuantization::Scalar(params) => params.shader_includes(),
            GpuQuantization::Product(params) => params.shader_includes(),
        }
    }

    fn shader_defines(&self) -> HashMap<String, Option<String>> {
        match self {
            GpuQuantization::Binary(params) => params.shader_defines(),
            GpuQuantization::Scalar(params) => params.shader_defines(),
            GpuQuantization::Product(params) => params.shader_defines(),
        }
    }
}

impl GpuQuantization {
    pub fn new_bq<T: BitsStoreType, TStorage: EncodedStorage>(
        device: Arc<gpu::Device>,
        quantized_storage: &EncodedVectorsBin<T, TStorage>,
    ) -> GpuQuantization {
        GpuQuantization::Binary(GpuBinaryQuantization::new(device, quantized_storage))
    }

    pub fn new_sq<TStorage: EncodedStorage>(
        device: Arc<gpu::Device>,
        quantized_storage: &EncodedVectorsU8<TStorage>,
    ) -> OperationResult<GpuQuantization> {
        Ok(GpuQuantization::Scalar(GpuScalarQuantization::new(
            device,
            quantized_storage,
        )?))
    }

    pub fn new_pq<TStorage: EncodedStorage>(
        device: Arc<gpu::Device>,
        quantized_storage: &EncodedVectorsPQ<TStorage>,
    ) -> OperationResult<GpuQuantization> {
        Ok(GpuQuantization::Product(GpuProductQuantization::new(
            device,
            quantized_storage,
        )?))
    }

    /// Adds additional bindings to descriptor set.
    pub fn add_descriptor_set(
        &self,
        descriptor_set_builder: gpu::DescriptorSetBuilder,
    ) -> gpu::DescriptorSetBuilder {
        match self {
            GpuQuantization::Binary(bq) => bq.add_descriptor_set(descriptor_set_builder),
            GpuQuantization::Scalar(sq) => sq.add_descriptor_set(descriptor_set_builder),
            GpuQuantization::Product(pq) => pq.add_descriptor_set(descriptor_set_builder),
        }
    }

    /// Adds additional bindings to descriptor set layout.
    pub fn add_descriptor_set_layout(
        &self,
        descriptor_set_layout_builder: gpu::DescriptorSetLayoutBuilder,
    ) -> gpu::DescriptorSetLayoutBuilder {
        match self {
            GpuQuantization::Binary(bq) => {
                bq.add_descriptor_set_layout(descriptor_set_layout_builder)
            }
            GpuQuantization::Scalar(sq) => {
                sq.add_descriptor_set_layout(descriptor_set_layout_builder)
            }
            GpuQuantization::Product(pq) => {
                pq.add_descriptor_set_layout(descriptor_set_layout_builder)
            }
        }
    }
}

/// Additional data for binary quantization in GPU.
/// It imnplements all shader definitions for BQ.
pub struct GpuBinaryQuantization {
    /// How many bits are added while aligning.
    /// We need to subtract them after XOR+popcnt operation.
    skip_count: usize,
}

impl ShaderBuilderParameters for GpuBinaryQuantization {
    fn shader_includes(&self) -> HashMap<String, String> {
        HashMap::from([(
            "vector_storage_bq.comp".to_string(),
            include_str!("../shaders/vector_storage_bq.comp").to_string(),
        )])
    }

    fn shader_defines(&self) -> HashMap<String, Option<String>> {
        let mut defines = HashMap::new();
        // Define that we are using quantization.
        defines.insert("VECTOR_STORAGE_QUANTIZATION".to_owned(), None);
        // Define that quantization is binary.
        defines.insert("VECTOR_STORAGE_ELEMENT_BQ".to_owned(), None);
        // Provide skip count for BQ.
        defines.insert(
            "BQ_SKIP_COUNT".to_owned(),
            Some(self.skip_count.to_string()),
        );
        defines
    }
}

impl GpuBinaryQuantization {
    fn new<T: BitsStoreType, TStorage: EncodedStorage>(
        device: Arc<gpu::Device>,
        quantized_storage: &EncodedVectorsBin<T, TStorage>,
    ) -> Self {
        let orig_dim = quantized_storage.get_vector_parameters().dim;
        // Bytes count for quantized vector.
        let quantized_vector_len = if quantized_storage.vectors_count() > 0 {
            quantized_storage.get_quantized_vector(0).len()
        } else {
            0
        };
        // Find bits count for aligned gpu vector.
        let gpu_bits_count = GpuVectorStorage::gpu_vector_capacity(&device, quantized_vector_len)
            * u8::BITS as usize;
        Self {
            skip_count: gpu_bits_count - orig_dim,
        }
    }

    #[allow(clippy::unused_self)]
    fn add_descriptor_set(
        &self,
        descriptor_set_builder: gpu::DescriptorSetBuilder,
    ) -> gpu::DescriptorSetBuilder {
        descriptor_set_builder
    }

    #[allow(clippy::unused_self)]
    fn add_descriptor_set_layout(
        &self,
        descriptor_set_layout_builder: gpu::DescriptorSetLayoutBuilder,
    ) -> gpu::DescriptorSetLayoutBuilder {
        descriptor_set_layout_builder
    }
}

pub struct GpuScalarQuantization {
    multiplier: f32,
    diff: f32,
    offsets_buffer: Arc<gpu::Buffer>,
    offsets_buffer_binding: usize,
}

impl ShaderBuilderParameters for GpuScalarQuantization {
    fn shader_includes(&self) -> HashMap<String, String> {
        HashMap::from([(
            "vector_storage_sq.comp".to_string(),
            include_str!("../shaders/vector_storage_sq.comp").to_string(),
        )])
    }

    fn shader_defines(&self) -> HashMap<String, Option<String>> {
        let mut defines = HashMap::new();
        // Define that we are using quantization.
        defines.insert("VECTOR_STORAGE_QUANTIZATION".to_owned(), None);
        // Define that quantization is scalar.
        defines.insert("VECTOR_STORAGE_ELEMENT_SQ".to_owned(), None);
        // Provide shader binding of SQ offsets.
        defines.insert(
            "VECTOR_STORAGE_SQ_OFFSETS_BINDING".to_owned(),
            Some(self.offsets_buffer_binding.to_string()),
        );
        // Provide multiplier and diff for quantization.
        defines.insert(
            "SQ_MULTIPLIER".to_owned(),
            Some(self.multiplier.to_string()),
        );
        defines.insert("SQ_DIFF".to_owned(), Some(self.diff.to_string()));
        defines
    }
}

impl GpuScalarQuantization {
    fn new<TStorage: EncodedStorage>(
        device: Arc<gpu::Device>,
        quantized_storage: &EncodedVectorsU8<TStorage>,
    ) -> OperationResult<Self> {
        Ok(GpuScalarQuantization {
            multiplier: quantized_storage.get_multiplier(),
            diff: quantized_storage.get_diff(),
            offsets_buffer: GpuScalarQuantization::create_sq_offsets_buffer(
                device,
                quantized_storage,
            )?,
            offsets_buffer_binding: START_QUANTIZATION_BINDING,
        })
    }

    fn create_sq_offsets_buffer<TStorage: EncodedStorage>(
        device: Arc<gpu::Device>,
        quantized_storage: &EncodedVectorsU8<TStorage>,
    ) -> OperationResult<Arc<gpu::Buffer>> {
        let sq_offsets_buffer = gpu::Buffer::new(
            device.clone(),
            "SQ offsets buffer",
            gpu::BufferType::Storage,
            quantized_storage.vectors_count() * std::mem::size_of::<f32>(),
        )?;

        let sq_offsets_staging_buffer = gpu::Buffer::new(
            device.clone(),
            "SQ offsets staging buffer",
            gpu::BufferType::CpuToGpu,
            sq_offsets_buffer.size(),
        )?;

        let mut upload_context = gpu::Context::new(device.clone())?;

        for i in 0..quantized_storage.vectors_count() {
            let (offset, _) = quantized_storage.get_quantized_vector(i as PointOffsetType);
            sq_offsets_staging_buffer.upload(&offset, i * std::mem::size_of::<f32>())?;
        }

        upload_context.copy_gpu_buffer(
            sq_offsets_staging_buffer,
            sq_offsets_buffer.clone(),
            0,
            0,
            sq_offsets_buffer.size(),
        )?;
        upload_context.run()?;
        upload_context.wait_finish(GPU_TIMEOUT)?;

        Ok(sq_offsets_buffer)
    }

    pub fn add_descriptor_set(
        &self,
        descriptor_set_builder: gpu::DescriptorSetBuilder,
    ) -> gpu::DescriptorSetBuilder {
        descriptor_set_builder
            .add_storage_buffer(self.offsets_buffer_binding, self.offsets_buffer.clone())
    }

    pub fn add_descriptor_set_layout(
        &self,
        descriptor_set_layout_builder: gpu::DescriptorSetLayoutBuilder,
    ) -> gpu::DescriptorSetLayoutBuilder {
        descriptor_set_layout_builder.add_storage_buffer(self.offsets_buffer_binding)
    }
}

pub struct GpuProductQuantization {
    centroids_buffer: Arc<gpu::Buffer>,
    centroids_buffer_binding: usize,
    vector_division_buffer: Arc<gpu::Buffer>,
    vector_division_buffer_binding: usize,
    divisions_count: usize,
    centroids_dim: usize,
}

impl ShaderBuilderParameters for GpuProductQuantization {
    fn shader_includes(&self) -> HashMap<String, String> {
        HashMap::from([(
            "vector_storage_pq.comp".to_string(),
            include_str!("../shaders/vector_storage_pq.comp").to_string(),
        )])
    }

    fn shader_defines(&self) -> HashMap<String, Option<String>> {
        let mut defines = HashMap::new();
        // Define that we are using quantization.
        defines.insert("VECTOR_STORAGE_QUANTIZATION".to_owned(), None);
        // Define that quantization is product.
        defines.insert("VECTOR_STORAGE_ELEMENT_PQ".to_owned(), None);
        // Provide shader binding of PQ centroids.
        defines.insert(
            "VECTOR_STORAGE_PQ_CENTROIDS_BINDING".to_owned(),
            Some(self.centroids_buffer_binding.to_string()),
        );
        // Provide shader binding of PQ vector division.
        defines.insert(
            "VECTOR_STORAGE_PQ_DIVISIONS_BINDING".to_owned(),
            Some(self.vector_division_buffer_binding.to_string()),
        );
        // Provide vector divisions count and centroids count for quantization.
        defines.insert(
            "PQ_DIVISIONS_COUNT".to_owned(),
            Some(self.divisions_count.to_string()),
        );
        defines.insert(
            "PQ_CENTROIDS_DIM".to_owned(),
            Some(self.centroids_dim.to_string()),
        );
        defines
    }
}

impl GpuProductQuantization {
    fn new<TStorage: EncodedStorage>(
        device: Arc<gpu::Device>,
        quantized_storage: &EncodedVectorsPQ<TStorage>,
    ) -> OperationResult<Self> {
        let centroids_buffer = gpu::Buffer::new(
            device.clone(),
            "PQ centroids buffer",
            gpu::BufferType::Storage,
            quantized_storage
                .get_metadata()
                .centroids
                .iter()
                .map(|c| c.len())
                .sum::<usize>()
                * std::mem::size_of::<f32>(),
        )?;
        let centroids_staging_buffer = gpu::Buffer::new(
            device.clone(),
            "PQ centroids staging buffer",
            gpu::BufferType::CpuToGpu,
            centroids_buffer.size(),
        )?;
        let vector_division_buffer = gpu::Buffer::new(
            device.clone(),
            "PQ vector division buffer",
            gpu::BufferType::Storage,
            quantized_storage.get_metadata().vector_division.len() * std::mem::size_of::<u32>() * 2,
        )?;
        let vector_division_staging_buffer = gpu::Buffer::new(
            device.clone(),
            "PQ vector division staging buffer",
            gpu::BufferType::CpuToGpu,
            vector_division_buffer.size(),
        )?;

        let mut upload_context = gpu::Context::new(device.clone())?;

        let mut centroids_offset = 0;
        for centroids in &quantized_storage.get_metadata().centroids {
            centroids_staging_buffer.upload_slice(centroids, centroids_offset)?;
            centroids_offset += centroids.len() * std::mem::size_of::<f32>();
        }

        upload_context.copy_gpu_buffer(
            centroids_staging_buffer,
            centroids_buffer.clone(),
            0,
            0,
            centroids_buffer.size(),
        )?;

        let vector_division: Vec<_> = quantized_storage
            .get_metadata()
            .vector_division
            .iter()
            .flat_map(|range| [range.start as u32, range.end as u32].into_iter())
            .collect();
        vector_division_staging_buffer.upload_slice(&vector_division, 0)?;

        upload_context.copy_gpu_buffer(
            vector_division_staging_buffer,
            vector_division_buffer.clone(),
            0,
            0,
            vector_division_buffer.size(),
        )?;

        upload_context.run()?;
        upload_context.wait_finish(GPU_TIMEOUT)?;

        Ok(Self {
            centroids_buffer,
            vector_division_buffer,
            divisions_count: quantized_storage.get_metadata().vector_division.len(),
            centroids_dim: quantized_storage
                .get_metadata()
                .centroids
                .first()
                .map(|c| c.len())
                .unwrap_or_default(),
            centroids_buffer_binding: START_QUANTIZATION_BINDING,
            vector_division_buffer_binding: START_QUANTIZATION_BINDING + 1,
        })
    }

    pub fn add_descriptor_set(
        &self,
        descriptor_set_builder: gpu::DescriptorSetBuilder,
    ) -> gpu::DescriptorSetBuilder {
        let descriptor_set_builder = descriptor_set_builder
            .add_storage_buffer(self.centroids_buffer_binding, self.centroids_buffer.clone());
        descriptor_set_builder.add_storage_buffer(
            self.vector_division_buffer_binding,
            self.vector_division_buffer.clone(),
        )
    }

    pub fn add_descriptor_set_layout(
        &self,
        descriptor_set_layout_builder: gpu::DescriptorSetLayoutBuilder,
    ) -> gpu::DescriptorSetLayoutBuilder {
        let descriptor_set_layout_builder =
            descriptor_set_layout_builder.add_storage_buffer(self.centroids_buffer_binding);
        descriptor_set_layout_builder.add_storage_buffer(self.vector_division_buffer_binding)
    }
}
