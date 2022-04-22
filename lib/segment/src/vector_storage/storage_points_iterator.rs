use crate::common::arc_atomic_ref_cell_iterator::ArcAtomicRefCellIterator;
use crate::id_tracker::points_iterator::PointsIterator;
use crate::types::PointOffsetType;
use crate::vector_storage::VectorStorageSS;
use atomic_refcell::AtomicRefCell;
use std::sync::Arc;

/// Implementation of points iterator through the vector storage
pub struct StoragePointsIterator(pub Arc<AtomicRefCell<VectorStorageSS>>);

impl PointsIterator for StoragePointsIterator {
    fn points_count(&self) -> usize {
        self.0.borrow().vector_count()
    }

    fn iter_ids(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        Box::new(ArcAtomicRefCellIterator::new(self.0.clone(), |storage| {
            storage.iter_ids()
        }))
    }

    fn max_id(&self) -> PointOffsetType {
        self.0.borrow().total_vector_count() as PointOffsetType
    }
}
