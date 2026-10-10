use std::collections::HashMap;

use crate::index::{UniversalReadExt, VectorIndexRead, VectorIndexType};
use crate::segment::Segment;
use crate::segment::read_only::ReadOnlySegment;
use crate::segment::vector_data_read::VectorDataRead;
use crate::types::VectorNameBuf;
use crate::vector_storage::VectorStorageRead;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VectorIndexInfo {
    pub index_type: VectorIndexType,
    /// Available vectors before proxy point-deletion masks.
    pub vectors_count: usize,
}

pub trait VectorIndexInfoProvider {
    fn vector_index_info(&self) -> HashMap<VectorNameBuf, VectorIndexInfo>;
}

fn vector_index_info<T: VectorDataRead>(
    vector_data: &HashMap<VectorNameBuf, T>,
) -> HashMap<VectorNameBuf, VectorIndexInfo> {
    vector_data
        .iter()
        .map(|(name, data)| {
            let info = VectorIndexInfo {
                index_type: data.vector_index().index_type(),
                vectors_count: data.vector_storage().available_vector_count(),
            };
            (name.clone(), info)
        })
        .collect()
}

impl VectorIndexInfoProvider for Segment {
    fn vector_index_info(&self) -> HashMap<VectorNameBuf, VectorIndexInfo> {
        vector_index_info(&self.vector_data)
    }
}

impl<S: UniversalReadExt + 'static> VectorIndexInfoProvider for ReadOnlySegment<S> {
    fn vector_index_info(&self) -> HashMap<VectorNameBuf, VectorIndexInfo> {
        vector_index_info(&self.vector_data)
    }
}
