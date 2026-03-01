#![cfg(feature = "gpu")]
use std::collections::HashMap;
use std::sync::Arc;

pub mod cuda;
pub mod vulkan;

// Re-export pure-data enums and Vulkan-only helpers that callers may use directly.
pub use vulkan::{
    AllocationCallbacks, BufferType, DebugMessenger, PanicIfErrorMessenger, PhysicalDeviceType,
};

// ─── Errors / Result ──────────────────────────────────────────────────────────

/// A trait for GPU resources.
/// It's used keep GPU resources alive while they are in use by the GPU context.
pub trait Resource: Send + Sync {}

#[derive(Debug)]
pub enum GpuError {
    OutOfMemory,
    OutOfBounds(String),
    NotSupported(String),
    NotReady,
    Timeout,
    Other(String),
}

pub type GpuResult<T> = Result<T, GpuError>;

impl From<gpu_allocator::AllocationError> for GpuError {
    fn from(error: gpu_allocator::AllocationError) -> GpuError {
        match error {
            gpu_allocator::AllocationError::OutOfMemory => GpuError::OutOfMemory,
            _ => GpuError::Other(format!("GPU allocator error: {error:?}")),
        }
    }
}

impl From<ash::vk::Result> for GpuError {
    fn from(result: ash::vk::Result) -> Self {
        use ash::vk;
        match result {
            vk::Result::NOT_READY => GpuError::NotReady,
            vk::Result::TIMEOUT => GpuError::Timeout,
            vk::Result::ERROR_OUT_OF_HOST_MEMORY => GpuError::OutOfMemory,
            vk::Result::ERROR_OUT_OF_DEVICE_MEMORY => GpuError::OutOfMemory,
            vk::Result::ERROR_LAYER_NOT_PRESENT => {
                GpuError::NotSupported("Layer is not present".to_string())
            }
            vk::Result::ERROR_EXTENSION_NOT_PRESENT => {
                GpuError::NotSupported("Extension is not present".to_string())
            }
            vk::Result::ERROR_FEATURE_NOT_PRESENT => {
                GpuError::NotSupported("Feature is not present".to_string())
            }
            vk::Result::ERROR_INCOMPATIBLE_DRIVER => {
                GpuError::NotSupported("Unable to find a Vulkan driver".to_string())
            }
            vk::Result::ERROR_FORMAT_NOT_SUPPORTED => {
                GpuError::NotSupported("Format is not supported".to_string())
            }
            _ => GpuError::Other(format!("Vulkan API error: {result:?}")),
        }
    }
}

// ─── Compiled shader result ───────────────────────────────────────────────────

/// Output of shader compilation — variant depends on the backend.
pub enum CompiledShader {
    /// SPIR-V bytecode for the Vulkan backend.
    Vulkan(Vec<u8>),
    /// HIP/CUDA binary blob + GlobalParams ordering for the CUDA/HIP backend.
    Cuda {
        binary: Vec<u8>,
        param_order: Vec<(usize, usize)>,
    },
}

// ─── PhysicalDevice ────────────────────────────────────────────────────────────

/// A physical GPU visible to the instance.
#[derive(Clone)]
pub enum PhysicalDevice {
    Vulkan(vulkan::PhysicalDevice),
    Cuda(cuda::CudaPhysicalDevice),
}

impl PhysicalDevice {
    pub fn name(&self) -> &str {
        match self {
            PhysicalDevice::Vulkan(p) => &p.name,
            PhysicalDevice::Cuda(p) => &p.name,
        }
    }

    pub fn device_type(&self) -> PhysicalDeviceType {
        match self {
            PhysicalDevice::Vulkan(p) => p.device_type,
            PhysicalDevice::Cuda(p) => p.device_type,
        }
    }
}

// ─── Instance ─────────────────────────────────────────────────────────────────

/// GPU instance — discovers physical devices and creates logical devices.
///
/// Backend selection priority: HIP → CUDA → Vulkan.
/// Override with `QDRANT_GPU_BACKEND=hip|cuda|vulkan`.
pub enum Instance {
    Vulkan(Arc<vulkan::Instance>),
    Cuda(Arc<cuda::CudaInstance>),
}

#[derive(Default)]
pub struct InstanceBuilder {
    debug_messenger: Option<Box<dyn vulkan::DebugMessenger>>,
    allocation_callbacks: Option<Box<dyn vulkan::AllocationCallbacks>>,
    dump_api: bool,
}

impl InstanceBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_debug_messenger(
        mut self,
        debug_messenger: Box<dyn vulkan::DebugMessenger>,
    ) -> Self {
        self.debug_messenger = Some(debug_messenger);
        self
    }

    pub fn with_allocation_callbacks(
        mut self,
        allocation_callbacks: Box<dyn vulkan::AllocationCallbacks>,
    ) -> Self {
        self.allocation_callbacks = Some(allocation_callbacks);
        self
    }

    pub fn with_dump_api(mut self, dump_api: bool) -> Self {
        self.dump_api = dump_api;
        self
    }

    pub fn build(self) -> GpuResult<Arc<Instance>> {
        // Check for forced backend via environment variable.
        let forced = std::env::var("QDRANT_GPU_BACKEND")
            .ok()
            .map(|s| s.to_lowercase());

        match forced.as_deref() {
            Some("hip") => {
                let inst = cuda::CudaInstance::try_hip()?;
                return Ok(Arc::new(Instance::Cuda(inst)));
            }
            Some("cuda") => {
                let inst = cuda::CudaInstance::try_cuda()?;
                return Ok(Arc::new(Instance::Cuda(inst)));
            }
            Some("vulkan") => {
                return Self::build_vulkan(
                    self.debug_messenger,
                    self.allocation_callbacks,
                    self.dump_api,
                );
            }
            _ => {}
        }

        // Auto-detect: HIP first, then CUDA, then Vulkan.
        if let Ok(inst) = cuda::CudaInstance::try_hip() {
            log::info!("Using HIP backend");
            return Ok(Arc::new(Instance::Cuda(inst)));
        }
        if let Ok(inst) = cuda::CudaInstance::try_cuda() {
            log::info!("Using CUDA backend");
            return Ok(Arc::new(Instance::Cuda(inst)));
        }

        log::info!("Using Vulkan backend");
        Self::build_vulkan(
            self.debug_messenger,
            self.allocation_callbacks,
            self.dump_api,
        )
    }

    fn build_vulkan(
        debug_messenger: Option<Box<dyn vulkan::DebugMessenger>>,
        allocation_callbacks: Option<Box<dyn vulkan::AllocationCallbacks>>,
        dump_api: bool,
    ) -> GpuResult<Arc<Instance>> {
        let vk_inst =
            vulkan::Instance::new_internal(debug_messenger, allocation_callbacks, dump_api)?;
        Ok(Arc::new(Instance::Vulkan(vk_inst)))
    }
}

impl Instance {
    pub fn builder() -> InstanceBuilder {
        InstanceBuilder::new()
    }

    pub fn physical_devices(&self) -> Vec<PhysicalDevice> {
        match self {
            Instance::Vulkan(inst) => inst
                .physical_devices()
                .iter()
                .map(|p| PhysicalDevice::Vulkan(p.clone()))
                .collect(),
            Instance::Cuda(inst) => inst
                .physical_devices()
                .iter()
                .map(|p| PhysicalDevice::Cuda(p.clone()))
                .collect(),
        }
    }

    /// Compile a Vulkan SPIR-V shader via slangc.
    /// For CUDA/HIP, use `Device::compile_shader_with_extra_args` instead.
    pub fn compile_shader(
        &self,
        shader: &str,
        shader_name: &str,
        defines: Option<&HashMap<String, Option<String>>>,
        includes: Option<&HashMap<String, String>>,
    ) -> GpuResult<CompiledShader> {
        self.compile_shader_with_extra_args(shader, shader_name, defines, includes, &[])
    }

    pub fn compile_shader_with_extra_args(
        &self,
        shader: &str,
        shader_name: &str,
        defines: Option<&HashMap<String, Option<String>>>,
        includes: Option<&HashMap<String, String>>,
        extra_args: &[&str],
    ) -> GpuResult<CompiledShader> {
        match self {
            Instance::Vulkan(inst) => {
                let spv = inst.compile_shader_with_extra_args(
                    shader,
                    shader_name,
                    defines,
                    includes,
                    extra_args,
                )?;
                Ok(CompiledShader::Vulkan(spv))
            }
            Instance::Cuda(_) => Err(GpuError::Other(
                "For CUDA/HIP, use Device::compile_shader_with_extra_args".to_string(),
            )),
        }
    }
}

// ─── Device ───────────────────────────────────────────────────────────────────

pub enum Device {
    Vulkan(Arc<vulkan::Device>),
    Cuda(Arc<cuda::CudaDevice>),
}

impl Device {
    pub fn new(
        instance: Arc<Instance>,
        physical_device: &PhysicalDevice,
    ) -> GpuResult<Arc<Device>> {
        Self::new_with_params(instance, physical_device, 0, false)
    }

    pub fn new_with_params(
        instance: Arc<Instance>,
        physical_device: &PhysicalDevice,
        queue_index: usize,
        skip_half_precision: bool,
    ) -> GpuResult<Arc<Device>> {
        match (instance.as_ref(), physical_device) {
            (Instance::Vulkan(inst), PhysicalDevice::Vulkan(pd)) => {
                let d = vulkan::Device::new_with_params(
                    inst.clone(),
                    pd,
                    queue_index,
                    skip_half_precision,
                )?;
                Ok(Arc::new(Device::Vulkan(d)))
            }
            (Instance::Cuda(_), PhysicalDevice::Cuda(cpd)) => {
                let d = cuda::CudaDevice::from_physical(cpd)?;
                Ok(Arc::new(Device::Cuda(d)))
            }
            _ => Err(GpuError::Other(
                "Instance and PhysicalDevice backend mismatch".to_string(),
            )),
        }
    }

    pub fn subgroup_size(&self) -> usize {
        match self {
            Device::Vulkan(d) => d.subgroup_size(),
            Device::Cuda(d) => d.subgroup_size(),
        }
    }

    pub fn max_buffer_size(&self) -> usize {
        match self {
            Device::Vulkan(d) => d.max_buffer_size(),
            Device::Cuda(d) => d.max_buffer_size(),
        }
    }

    pub fn max_compute_work_group_count(&self) -> [usize; 3] {
        match self {
            Device::Vulkan(d) => d.max_compute_work_group_count(),
            Device::Cuda(d) => d.max_compute_work_group_count(),
        }
    }

    pub fn has_half_precision(&self) -> bool {
        match self {
            Device::Vulkan(d) => d.has_half_precision(),
            Device::Cuda(d) => d.has_half_precision(),
        }
    }

    pub fn has_buffer_device_address(&self) -> bool {
        match self {
            Device::Vulkan(d) => d.has_buffer_device_address(),
            Device::Cuda(_) => true,
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Device::Vulkan(d) => d.name(),
            Device::Cuda(d) => d.name(),
        }
    }

    /// Compile a shader for this device's backend.
    /// For Vulkan: slangc → SPIR-V.
    /// For CUDA/HIP: slangc → .cu → hipcc/nvcc → binary.
    pub fn compile_shader_with_extra_args(
        &self,
        shader: &str,
        shader_name: &str,
        defines: Option<&HashMap<String, Option<String>>>,
        includes: Option<&HashMap<String, String>>,
        extra_args: &[&str],
    ) -> GpuResult<CompiledShader> {
        match self {
            Device::Vulkan(d) => {
                let spv = d.instance().compile_shader_with_extra_args(
                    shader,
                    shader_name,
                    defines,
                    includes,
                    extra_args,
                )?;
                Ok(CompiledShader::Vulkan(spv))
            }
            Device::Cuda(d) => {
                let cs = cuda::compile::compile_shader(shader, shader_name, defines, includes, d)?;
                Ok(CompiledShader::Cuda {
                    binary: cs.binary,
                    param_order: cs.param_order,
                })
            }
        }
    }
}

// ─── Buffer ───────────────────────────────────────────────────────────────────

pub enum Buffer {
    Vulkan(Arc<vulkan::Buffer>),
    Cuda(Arc<cuda::CudaBuffer>),
}

impl Buffer {
    pub fn new(
        device: Arc<Device>,
        name: impl AsRef<str>,
        buffer_type: BufferType,
        size: usize,
    ) -> GpuResult<Arc<Self>> {
        match device.as_ref() {
            Device::Vulkan(d) => {
                let b = vulkan::Buffer::new(d.clone(), name, buffer_type, size)?;
                Ok(Arc::new(Buffer::Vulkan(b)))
            }
            Device::Cuda(d) => {
                let b = cuda::CudaBuffer::new(d.clone(), name, buffer_type, size)?;
                Ok(Arc::new(Buffer::Cuda(b)))
            }
        }
    }

    pub fn size(&self) -> usize {
        match self {
            Buffer::Vulkan(b) => b.size(),
            Buffer::Cuda(b) => b.size(),
        }
    }

    pub fn buffer_type(&self) -> BufferType {
        match self {
            Buffer::Vulkan(b) => b.buffer_type(),
            Buffer::Cuda(b) => b.buffer_type(),
        }
    }

    pub fn device_address(&self) -> u64 {
        match self {
            Buffer::Vulkan(b) => b.device_address(),
            Buffer::Cuda(b) => b.device_address(),
        }
    }

    pub fn upload<T>(&self, data: &T, offset: usize) -> GpuResult<()>
    where
        T: zerocopy::IntoBytes + zerocopy::Immutable + ?Sized,
    {
        match self {
            Buffer::Vulkan(b) => b.upload(data, offset),
            Buffer::Cuda(b) => b.upload(data, offset),
        }
    }

    pub fn download<T>(&self, data: &mut T, offset: usize) -> GpuResult<()>
    where
        T: zerocopy::FromBytes + zerocopy::IntoBytes + ?Sized,
    {
        match self {
            Buffer::Vulkan(b) => b.download(data, offset),
            Buffer::Cuda(b) => b.download(data, offset),
        }
    }

    pub fn download_vec<T>(&self, offset: usize, len: usize) -> GpuResult<Vec<T>>
    where
        T: zerocopy::FromBytes + zerocopy::IntoBytes + Clone,
    {
        match self {
            Buffer::Vulkan(b) => b.download_vec(offset, len),
            Buffer::Cuda(b) => b.download_vec(offset, len),
        }
    }
}

impl Resource for Buffer {}

// ─── Shader ───────────────────────────────────────────────────────────────────

pub enum Shader {
    Vulkan(Arc<vulkan::Shader>),
    Cuda(Arc<cuda::CudaShader>),
}

impl Shader {
    pub fn new(device: Arc<Device>, compiled: &CompiledShader) -> GpuResult<Arc<Self>> {
        match (device.as_ref(), compiled) {
            (Device::Vulkan(d), CompiledShader::Vulkan(spv)) => {
                let s = vulkan::Shader::new(d.clone(), spv)?;
                Ok(Arc::new(Shader::Vulkan(s)))
            }
            (
                Device::Cuda(d),
                CompiledShader::Cuda {
                    binary,
                    param_order,
                },
            ) => {
                let s = cuda::CudaShader::new(d.clone(), binary.clone(), param_order.clone())?;
                Ok(Arc::new(Shader::Cuda(s)))
            }
            _ => Err(GpuError::Other(
                "Device/shader backend mismatch".to_string(),
            )),
        }
    }
}

impl Resource for Shader {}

// ─── DescriptorSetLayout ──────────────────────────────────────────────────────

pub enum DescriptorSetLayout {
    Vulkan(Arc<vulkan::DescriptorSetLayout>),
    Cuda(Arc<cuda::CudaDescriptorSetLayout>),
}

pub struct DescriptorSetLayoutBuilder {
    uniform_buffer_bindings: Vec<usize>,
    storage_buffer_bindings: Vec<usize>,
}

impl DescriptorSetLayoutBuilder {
    pub fn add_uniform_buffer(mut self, binding: usize) -> Self {
        self.uniform_buffer_bindings.push(binding);
        self
    }

    pub fn add_storage_buffer(mut self, binding: usize) -> Self {
        self.storage_buffer_bindings.push(binding);
        self
    }

    pub fn build(&self, device: Arc<Device>) -> GpuResult<Arc<DescriptorSetLayout>> {
        match device.as_ref() {
            Device::Vulkan(d) => {
                let mut builder = vulkan::DescriptorSetLayout::builder();
                for &b in &self.uniform_buffer_bindings {
                    builder = builder.add_uniform_buffer(b);
                }
                for &b in &self.storage_buffer_bindings {
                    builder = builder.add_storage_buffer(b);
                }
                let layout = builder.build(d.clone())?;
                Ok(Arc::new(DescriptorSetLayout::Vulkan(layout)))
            }
            Device::Cuda(d) => {
                let mut builder = cuda::CudaDescriptorSetLayout::builder();
                for &b in &self.uniform_buffer_bindings {
                    builder = builder.add_uniform_buffer(b);
                }
                for &b in &self.storage_buffer_bindings {
                    builder = builder.add_storage_buffer(b);
                }
                let layout = builder.build(d.clone())?;
                Ok(Arc::new(DescriptorSetLayout::Cuda(layout)))
            }
        }
    }
}

impl DescriptorSetLayout {
    pub fn builder() -> DescriptorSetLayoutBuilder {
        DescriptorSetLayoutBuilder {
            uniform_buffer_bindings: Vec::new(),
            storage_buffer_bindings: Vec::new(),
        }
    }

    pub fn device(&self) -> Arc<Device> {
        match self {
            DescriptorSetLayout::Vulkan(l) => Arc::new(Device::Vulkan(l.device())),
            DescriptorSetLayout::Cuda(l) => Arc::new(Device::Cuda(l.device())),
        }
    }
}

// ─── DescriptorSet ────────────────────────────────────────────────────────────

pub enum DescriptorSet {
    Vulkan(Arc<vulkan::DescriptorSet>),
    Cuda(Arc<cuda::CudaDescriptorSet>),
}

pub struct DescriptorSetBuilder {
    layout: Arc<DescriptorSetLayout>,
    uniform_buffers: Vec<(usize, Arc<Buffer>)>,
    storage_buffers: Vec<(usize, Arc<Buffer>)>,
}

impl DescriptorSetBuilder {
    pub fn add_uniform_buffer(mut self, binding: usize, buffer: Arc<Buffer>) -> Self {
        self.uniform_buffers.push((binding, buffer));
        self
    }

    pub fn add_storage_buffer(mut self, binding: usize, buffer: Arc<Buffer>) -> Self {
        self.storage_buffers.push((binding, buffer));
        self
    }

    pub fn build(&self) -> GpuResult<Arc<DescriptorSet>> {
        match self.layout.as_ref() {
            DescriptorSetLayout::Vulkan(layout) => {
                let mut builder = vulkan::DescriptorSet::builder(layout.clone());
                for (binding, buf) in &self.uniform_buffers {
                    match buf.as_ref() {
                        Buffer::Vulkan(b) => {
                            builder = builder.add_uniform_buffer(*binding, b.clone())
                        }
                        _ => {
                            return Err(GpuError::Other(
                                "Buffer/layout backend mismatch".to_string(),
                            ));
                        }
                    }
                }
                for (binding, buf) in &self.storage_buffers {
                    match buf.as_ref() {
                        Buffer::Vulkan(b) => {
                            builder = builder.add_storage_buffer(*binding, b.clone())
                        }
                        _ => {
                            return Err(GpuError::Other(
                                "Buffer/layout backend mismatch".to_string(),
                            ));
                        }
                    }
                }
                Ok(Arc::new(DescriptorSet::Vulkan(builder.build()?)))
            }
            DescriptorSetLayout::Cuda(layout) => {
                let mut builder = cuda::CudaDescriptorSet::builder(layout.clone());
                for (binding, buf) in &self.uniform_buffers {
                    match buf.as_ref() {
                        Buffer::Cuda(b) => {
                            builder = builder.add_uniform_buffer(*binding, b.clone())
                        }
                        _ => {
                            return Err(GpuError::Other(
                                "Buffer/layout backend mismatch".to_string(),
                            ));
                        }
                    }
                }
                for (binding, buf) in &self.storage_buffers {
                    match buf.as_ref() {
                        Buffer::Cuda(b) => {
                            builder = builder.add_storage_buffer(*binding, b.clone())
                        }
                        _ => {
                            return Err(GpuError::Other(
                                "Buffer/layout backend mismatch".to_string(),
                            ));
                        }
                    }
                }
                Ok(Arc::new(DescriptorSet::Cuda(builder.build()?)))
            }
        }
    }
}

impl DescriptorSet {
    pub fn builder(layout: Arc<DescriptorSetLayout>) -> DescriptorSetBuilder {
        DescriptorSetBuilder {
            layout,
            uniform_buffers: Vec::new(),
            storage_buffers: Vec::new(),
        }
    }
}

impl Resource for DescriptorSet {}

// ─── Pipeline ─────────────────────────────────────────────────────────────────

pub enum Pipeline {
    Vulkan(Arc<vulkan::Pipeline>),
    Cuda(Arc<cuda::CudaPipeline>),
}

pub struct PipelineBuilder {
    shader: Option<Arc<Shader>>,
    descriptor_set_layouts: HashMap<usize, Arc<DescriptorSetLayout>>,
}

impl Default for PipelineBuilder {
    fn default() -> Self {
        PipelineBuilder {
            shader: None,
            descriptor_set_layouts: HashMap::new(),
        }
    }
}

impl PipelineBuilder {
    pub fn add_shader(mut self, shader: Arc<Shader>) -> Self {
        self.shader = Some(shader);
        self
    }

    pub fn add_descriptor_set_layout(
        mut self,
        set: usize,
        layout: Arc<DescriptorSetLayout>,
    ) -> Self {
        self.descriptor_set_layouts.insert(set, layout);
        self
    }

    pub fn build(&self, device: Arc<Device>) -> GpuResult<Arc<Pipeline>> {
        let shader = self
            .shader
            .as_ref()
            .ok_or_else(|| GpuError::Other("Pipeline requires a shader".to_string()))?;
        match (device.as_ref(), shader.as_ref()) {
            (Device::Vulkan(d), Shader::Vulkan(s)) => {
                let mut builder = vulkan::Pipeline::builder().add_shader(s.clone());
                for (&set, layout) in &self.descriptor_set_layouts {
                    match layout.as_ref() {
                        DescriptorSetLayout::Vulkan(l) => {
                            builder = builder.add_descriptor_set_layout(set, l.clone());
                        }
                        _ => {
                            return Err(GpuError::Other("Backend mismatch in pipeline".to_string()));
                        }
                    }
                }
                Ok(Arc::new(Pipeline::Vulkan(builder.build(d.clone())?)))
            }
            (Device::Cuda(d), Shader::Cuda(s)) => {
                let p = cuda::CudaPipeline::builder()
                    .add_shader(s.clone())
                    .build(d.clone())?;
                Ok(Arc::new(Pipeline::Cuda(p)))
            }
            _ => Err(GpuError::Other(
                "Device/shader backend mismatch".to_string(),
            )),
        }
    }
}

impl Pipeline {
    pub fn builder() -> PipelineBuilder {
        PipelineBuilder::default()
    }
}

impl Resource for Pipeline {}

// ─── Context ──────────────────────────────────────────────────────────────────

pub enum Context {
    Vulkan(vulkan::Context),
    Cuda(cuda::CudaContext),
}

impl Context {
    pub fn new(device: Arc<Device>) -> GpuResult<Self> {
        match device.as_ref() {
            Device::Vulkan(d) => Ok(Context::Vulkan(vulkan::Context::new(d.clone())?)),
            Device::Cuda(d) => Ok(Context::Cuda(cuda::CudaContext::new(d.clone())?)),
        }
    }

    pub fn bind_pipeline(
        &mut self,
        pipeline: Arc<Pipeline>,
        descriptor_sets: &[Arc<DescriptorSet>],
    ) -> GpuResult<()> {
        match self {
            Context::Vulkan(ctx) => {
                let vk_p = match pipeline.as_ref() {
                    Pipeline::Vulkan(p) => p.clone(),
                    _ => {
                        return Err(GpuError::Other(
                            "Backend mismatch in bind_pipeline".to_string(),
                        ));
                    }
                };
                let vk_sets: GpuResult<Vec<_>> = descriptor_sets
                    .iter()
                    .map(|s| match s.as_ref() {
                        DescriptorSet::Vulkan(ds) => Ok(ds.clone()),
                        _ => Err(GpuError::Other(
                            "Backend mismatch in descriptor set".to_string(),
                        )),
                    })
                    .collect();
                ctx.bind_pipeline(vk_p, &vk_sets?)
            }
            Context::Cuda(ctx) => {
                let cu_p = match pipeline.as_ref() {
                    Pipeline::Cuda(p) => p.clone(),
                    _ => {
                        return Err(GpuError::Other(
                            "Backend mismatch in bind_pipeline".to_string(),
                        ));
                    }
                };
                let cu_sets: GpuResult<Vec<_>> = descriptor_sets
                    .iter()
                    .map(|s| match s.as_ref() {
                        DescriptorSet::Cuda(ds) => Ok(ds.clone()),
                        _ => Err(GpuError::Other(
                            "Backend mismatch in descriptor set".to_string(),
                        )),
                    })
                    .collect();
                ctx.bind_pipeline(cu_p, &cu_sets?)
            }
        }
    }

    pub fn dispatch(&mut self, x: usize, y: usize, z: usize) -> GpuResult<()> {
        match self {
            Context::Vulkan(ctx) => ctx.dispatch(x, y, z),
            Context::Cuda(ctx) => ctx.dispatch(x, y, z),
        }
    }

    pub fn copy_gpu_buffer(
        &mut self,
        src: Arc<Buffer>,
        dst: Arc<Buffer>,
        src_offset: usize,
        dst_offset: usize,
        size: usize,
    ) -> GpuResult<()> {
        match self {
            Context::Vulkan(ctx) => {
                let s = match src.as_ref() {
                    Buffer::Vulkan(b) => b.clone(),
                    _ => return Err(GpuError::Other("Backend mismatch".to_string())),
                };
                let d = match dst.as_ref() {
                    Buffer::Vulkan(b) => b.clone(),
                    _ => return Err(GpuError::Other("Backend mismatch".to_string())),
                };
                ctx.copy_gpu_buffer(s, d, src_offset, dst_offset, size)
            }
            Context::Cuda(ctx) => {
                let s = match src.as_ref() {
                    Buffer::Cuda(b) => b.clone(),
                    _ => return Err(GpuError::Other("Backend mismatch".to_string())),
                };
                let d = match dst.as_ref() {
                    Buffer::Cuda(b) => b.clone(),
                    _ => return Err(GpuError::Other("Backend mismatch".to_string())),
                };
                ctx.copy_gpu_buffer(s, d, src_offset, dst_offset, size)
            }
        }
    }

    pub fn clear_buffer(&mut self, buffer: Arc<Buffer>) -> GpuResult<()> {
        match self {
            Context::Vulkan(ctx) => match buffer.as_ref() {
                Buffer::Vulkan(b) => ctx.clear_buffer(b.clone()),
                _ => Err(GpuError::Other(
                    "Backend mismatch in clear_buffer".to_string(),
                )),
            },
            Context::Cuda(ctx) => match buffer.as_ref() {
                Buffer::Cuda(b) => ctx.clear_buffer(b.clone()),
                _ => Err(GpuError::Other(
                    "Backend mismatch in clear_buffer".to_string(),
                )),
            },
        }
    }

    pub fn barrier_buffers(&mut self, buffers: &[Arc<Buffer>]) -> GpuResult<()> {
        match self {
            Context::Vulkan(ctx) => {
                let vk_bufs: GpuResult<Vec<_>> = buffers
                    .iter()
                    .map(|b| match b.as_ref() {
                        Buffer::Vulkan(vb) => Ok(vb.clone()),
                        _ => Err(GpuError::Other("Backend mismatch".to_string())),
                    })
                    .collect();
                ctx.barrier_buffers(&vk_bufs?)
            }
            Context::Cuda(ctx) => {
                let cu_bufs: GpuResult<Vec<_>> = buffers
                    .iter()
                    .map(|b| match b.as_ref() {
                        Buffer::Cuda(cb) => Ok(cb.clone()),
                        _ => Err(GpuError::Other("Backend mismatch".to_string())),
                    })
                    .collect();
                ctx.barrier_buffers(&cu_bufs?)
            }
        }
    }

    pub fn run(&mut self) -> GpuResult<()> {
        match self {
            Context::Vulkan(ctx) => ctx.run(),
            Context::Cuda(ctx) => ctx.run(),
        }
    }

    pub fn wait_finish(&mut self, timeout: std::time::Duration) -> GpuResult<()> {
        match self {
            Context::Vulkan(ctx) => ctx.wait_finish(timeout),
            Context::Cuda(ctx) => ctx.wait_finish(timeout),
        }
    }
}

// ─── Test helpers ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod basic_test;

#[cfg(any(test, feature = "testing"))]
pub static GPU_TEST_INSTANCE: std::sync::LazyLock<Arc<Instance>> = std::sync::LazyLock::new(|| {
    Instance::builder()
        .with_debug_messenger(Box::new(PanicIfErrorMessenger {}))
        .build()
        .unwrap()
});
