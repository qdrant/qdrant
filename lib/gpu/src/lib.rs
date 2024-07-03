pub mod context;
pub use context::*;

pub mod debug_messenger;
pub use debug_messenger::*;

pub mod descriptor_set;
pub use descriptor_set::*;

pub mod descriptor_set_layout;
pub use descriptor_set_layout::*;

pub mod buffer;
pub use buffer::*;

pub mod device;
pub use device::*;

pub mod instance;
pub use instance::*;

pub mod pipeline;
pub use pipeline::*;

pub mod pipeline_builder;
pub use pipeline_builder::*;

pub mod shader;
pub use shader::*;

pub trait Resource: Send + Sync {}

#[derive(Debug)]
pub enum GpuError {
    AllocationError(gpu_allocator::AllocationError),
    NotSupported,
}

pub type GpuResult<T> = Result<T, GpuError>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GpuVectorStorageElementType {
    Float32,
    Float16,
    Uint8,
    Binary,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LayoutSetBinding {
    VisitedFlags,
    VectorStorage,
    NearestHeap,
    Links,
    CandidatesHeap,
}

impl LayoutSetBinding {
    pub fn to_string(self) -> &'static str {
        match self {
            LayoutSetBinding::VisitedFlags => "VISITED_FLAGS_LAYOUT_SET",
            LayoutSetBinding::VectorStorage => "VECTOR_STORAGE_LAYOUT_SET",
            LayoutSetBinding::NearestHeap => "NEAREST_HEAP_LAYOUT_SET",
            LayoutSetBinding::Links => "LINKS_LAYOUT_SET",
            LayoutSetBinding::CandidatesHeap => "CANDIDATES_HEAP_LAYOUT_SET",
        }
    }
}
