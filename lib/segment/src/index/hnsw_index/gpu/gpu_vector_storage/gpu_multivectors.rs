use std::collections::HashMap;
use std::sync::Arc;

use common::types::PointOffsetType;
use quantization::EncodedVectors;

use super::gpu_quantization::MAX_QUANTIZATION_BINDINGS;
use super::STORAGES_COUNT;
use crate::common::operation_error::OperationResult;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::index::hnsw_index::gpu::shader_builder::ShaderBuilderParameters;
use crate::index::hnsw_index::gpu::GPU_TIMEOUT;
use crate::vector_storage::quantized::quantized_multivector_storage::{
    MultivectorOffsetsStorage, QuantizedMultivectorStorage,
};
use crate::vector_storage::MultiVectorStorage;

// Multivector shader binding is after vectot data and quantization data bindings.
const START_MULTIVECTORS_BINDING: usize = STORAGES_COUNT + MAX_QUANTIZATION_BINDINGS;

/// Shader struct for multivector offsets with start id and count of vectors in multivector.
#[repr(C)]
struct GpuMultivectorOffset {
    start: u32,
    count: u32,
}

/// Gpu data for multivectors
pub struct GpuMultivectors {
    /// Positions of multivector offsets.
    offsets_buffer: Arc<gpu::Buffer>,
    /// Shader binding for offsets.
    offsets_binding: usize,
}

impl ShaderBuilderParameters for GpuMultivectors {
    fn shader_includes(&self) -> HashMap<String, String> {
        // No additional includes for multivectors.
        // Cause all multivector logic is defined in `vector_storage.comp` shader.
        Default::default()
    }

    fn shader_defines(&self) -> HashMap<String, Option<String>> {
        let mut defines = HashMap::new();
        // Set enabled flag for multivectors.
        defines.insert("MULTIVECTORS".to_owned(), None);
        // Provide shader binding of multivector offsets.
        defines.insert(
            "MULTIVECTOR_OFFSETS_BINDING".to_owned(),
            Some(self.offsets_binding.to_string()),
        );
        defines
    }
}

impl GpuMultivectors {
    /// Construct multivectors data from quantized storage.
    pub fn new_quantized<
        TEncodedQuery: Sized,
        QuantizedStorage: EncodedVectors<TEncodedQuery>,
        TMultivectorOffsetsStorage: MultivectorOffsetsStorage,
    >(
        device: Arc<gpu::Device>,
        quantized_storage: &QuantizedMultivectorStorage<
            TEncodedQuery,
            QuantizedStorage,
            TMultivectorOffsetsStorage,
        >,
    ) -> OperationResult<GpuMultivectors> {
        Self::new_impl(
            device,
            (0..quantized_storage.vectors_count())
                .map(|id| quantized_storage.inner_vector_offset(id as PointOffsetType))
                .map(|x| GpuMultivectorOffset {
                    start: x.start,
                    count: x.count,
                }),
        )
    }

    /// Construct multivectors data from vector storage.
    pub fn new_multidense<T: PrimitiveVectorElement, TVectorStorage: MultiVectorStorage<T>>(
        device: Arc<gpu::Device>,
        vector_storage: &TVectorStorage,
    ) -> OperationResult<GpuMultivectors> {
        Self::new_impl(
            device,
            (0..vector_storage.total_vector_count())
                // map ID to count of vectors in multivector
                .map(|id| {
                    vector_storage
                        .get_multi(id as PointOffsetType)
                        .vectors_count()
                })
                // Map count of vectors to start and count of vectors in multivector.
                .scan(0, |acc, count| {
                    let start = *acc;
                    *acc += count;
                    Some(GpuMultivectorOffset {
                        start: start as u32,
                        count: count as u32,
                    })
                }),
        )
    }

    /// Adds multivector data to the descriptor set builder.
    pub fn add_descriptor_set(
        &self,
        descriptor_set_builder: gpu::DescriptorSetBuilder,
    ) -> gpu::DescriptorSetBuilder {
        descriptor_set_builder.add_storage_buffer(self.offsets_binding, self.offsets_buffer.clone())
    }

    /// Adds multivector data to the descriptor set layout builder.
    pub fn add_descriptor_set_layout(
        &self,
        descriptor_set_layout_builder: gpu::DescriptorSetLayoutBuilder,
    ) -> gpu::DescriptorSetLayoutBuilder {
        descriptor_set_layout_builder.add_storage_buffer(self.offsets_binding)
    }

    fn new_impl(
        device: Arc<gpu::Device>,
        vector_offsets: impl Iterator<Item = GpuMultivectorOffset> + Clone,
    ) -> OperationResult<GpuMultivectors> {
        let multivectors_count = vector_offsets.clone().count();
        let offsets = gpu::Buffer::new(
            device.clone(),
            "Multivector offsets buffer",
            gpu::BufferType::Storage,
            multivectors_count * std::mem::size_of::<GpuMultivectorOffset>(),
        )?;
        let offsets_staging_buffer = gpu::Buffer::new(
            device.clone(),
            "Multivector offsets staging buffer",
            gpu::BufferType::CpuToGpu,
            offsets.size(),
        )?;
        for (point_id, offset) in vector_offsets.enumerate() {
            offsets_staging_buffer.upload(
                &offset,
                point_id * std::mem::size_of::<GpuMultivectorOffset>(),
            )?;
        }

        let mut upload_context = gpu::Context::new(device.clone())?;
        upload_context.copy_gpu_buffer(
            offsets_staging_buffer,
            offsets.clone(),
            0,
            0,
            offsets.size(),
        )?;
        upload_context.run()?;
        upload_context.wait_finish(GPU_TIMEOUT)?;

        Ok(GpuMultivectors {
            offsets_buffer: offsets,
            offsets_binding: START_MULTIVECTORS_BINDING,
        })
    }
}
