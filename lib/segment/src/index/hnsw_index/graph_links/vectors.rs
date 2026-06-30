//! Access to base/link vector values during link serialization.
//!
//! The `CompressedWithVectors` format inlines quantized vectors next to the
//! links. [`GraphLinksVectors`] is the abstraction [`serialize_graph_links`]
//! uses to read those vectors; [`StorageGraphLinksVectors`] is the production
//! implementation backed by the real vector/quantized storages.
//!
//! [`serialize_graph_links`]: super::serialize_graph_links

use std::alloc::Layout;
use std::borrow::Cow;

use common::generic_consts::Sequential;
use common::types::PointOffsetType;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead};

/// This trait lets the [`serialize_graph_links`] to access vector values.
///
/// [`serialize_graph_links`]: super::serialize_graph_links
pub trait GraphLinksVectors {
    /// Call `f` with the raw bytes of the base vector for `point_id`.
    ///
    /// Base vectors will be included once per point on level 0.
    /// The layout of each vector must correspond to [`GraphLinksVectorsLayout::base`].
    fn for_base_vector(
        &self,
        point_id: PointOffsetType,
        f: &mut dyn FnMut(&[u8]) -> OperationResult<()>,
    ) -> OperationResult<()>;

    /// Link vectors will be included for each link per point.
    /// The layout of each vector must correspond to [`GraphLinksVectorsLayout::link`].
    fn get_link_vector(&self, point_id: PointOffsetType) -> OperationResult<Cow<'_, [u8]>>;

    /// Get the layout of base and link vectors.
    fn vectors_layout(&self) -> GraphLinksVectorsLayout;
}

/// Layout of base and link vectors, returned by [`GraphLinksVectors::vectors_layout`].
#[derive(Copy, Clone)]
pub struct GraphLinksVectorsLayout {
    pub base: Layout,
    pub link: Layout,
}

/// A [`GraphLinksVectors`] implementation that uses real storage.
pub struct StorageGraphLinksVectors<'a> {
    vector_storage: &'a VectorStorageEnum,   // base vectors
    quantized_vectors: &'a QuantizedVectors, // link vectors
    vectors_layout: GraphLinksVectorsLayout,
}

impl<'a> StorageGraphLinksVectors<'a> {
    pub fn try_new(
        vector_storage: &'a VectorStorageEnum,
        quantized_vectors: Option<&'a QuantizedVectors>,
    ) -> Option<Self> {
        let quantized_vectors = quantized_vectors?;
        Some(Self {
            vector_storage,
            quantized_vectors,
            vectors_layout: GraphLinksVectorsLayout {
                base: vector_storage.get_vector_layout().ok()?,
                link: quantized_vectors.get_quantized_vector_layout().ok()?,
            },
        })
    }
}

impl<'a> GraphLinksVectors for StorageGraphLinksVectors<'a> {
    /// Note: uses [`Sequential`] because [`serialize_graph_links`]
    /// traverses base vectors in a sequential order.
    ///
    /// [`serialize_graph_links`]: super::serialize_graph_links
    fn for_base_vector(
        &self,
        point_id: PointOffsetType,
        f: &mut dyn FnMut(&[u8]) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.vector_storage
            .with_vector_bytes_opt::<Sequential, _>(point_id, f)
            .unwrap_or_else(|| {
                Err(OperationError::service_error(format!(
                    "Point {point_id} not found in vector storage"
                )))
            })
    }

    /// Note: unlike base vectors, link vectors are written in a random order.
    fn get_link_vector(&self, point_id: PointOffsetType) -> OperationResult<Cow<'_, [u8]>> {
        Ok(self.quantized_vectors.get_quantized_vector(point_id))
    }

    fn vectors_layout(&self) -> GraphLinksVectorsLayout {
        self.vectors_layout
    }
}
