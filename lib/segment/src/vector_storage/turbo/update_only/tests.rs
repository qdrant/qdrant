//! Writes through the update-only storage, then reads back through the ordinary
//! appendable TurboQuant storage opened on the same directory.

use common::counter::hardware_counter::HardwareCounterCell;
use common::universal_io::MmapFs;
use tempfile::TempDir;

use super::UpdateOnlyTurboVectorStorage;
use crate::data_types::vectors::{VectorElementType, VectorRef};
use crate::types::Distance;
use crate::vector_storage::VectorStorageRead;
use crate::vector_storage::turbo::appendable_turbo_vector_storage::open_appendable_turbo_vector_storage;
use crate::vector_storage::update_only::VectorToStore;

type Writer = UpdateOnlyTurboVectorStorage;

const DIM: usize = 8;

/// The encoding this writer produces is the one the storage reads: the encoded
/// bytes match what the writable side stores for the same vector.
#[test]
fn encoded_vectors_match_the_writable_side() {
    let vector: Vec<VectorElementType> = (0..DIM).map(|i| i as f32 + 0.5).collect();
    let hw_counter = HardwareCounterCell::new();

    // Written by the update-only writer.
    let ours = TempDir::with_prefix("update_only_turbo").unwrap();
    let mut writer = Writer::open(&MmapFs, ours.path(), DIM, Distance::Dot).unwrap();
    writer
        .append_many(
            &MmapFs,
            0,
            [
                VectorToStore::Decoded(VectorRef::from(vector.as_slice())),
                VectorToStore::Missing,
            ],
            &hw_counter,
        )
        .unwrap();
    drop(writer);

    // Written by the storage itself.
    let theirs = TempDir::with_prefix("turbo_reference").unwrap();
    let mut reference =
        open_appendable_turbo_vector_storage(theirs.path(), DIM, Distance::Dot, false).unwrap();
    {
        use crate::vector_storage::VectorStorage as _;
        reference
            .insert_vector(0, VectorRef::from(vector.as_slice()), &hw_counter)
            .unwrap();
    }

    let ours =
        open_appendable_turbo_vector_storage(ours.path(), DIM, Distance::Dot, false).unwrap();
    assert_eq!(
        ours.get_quantized_vector(0),
        reference.get_quantized_vector(0),
        "the update-only writer must encode exactly as the storage does",
    );

    assert!(!ours.is_deleted_vector(0));
    assert!(
        ours.is_deleted_vector(1),
        "the missing vector took its slot"
    );
}

/// A second writer resumes above what the first left.
#[test]
fn batches_resume() {
    let dir = TempDir::with_prefix("update_only_turbo").unwrap();
    let hw_counter = HardwareCounterCell::new();
    let vector: Vec<VectorElementType> = vec![1.0; DIM];

    for slot in 0..2 {
        let mut writer = Writer::open(&MmapFs, dir.path(), DIM, Distance::Dot).unwrap();
        writer
            .append_many(
                &MmapFs,
                slot,
                [VectorToStore::Decoded(VectorRef::from(vector.as_slice()))],
                &hw_counter,
            )
            .unwrap();
    }

    let storage =
        open_appendable_turbo_vector_storage(dir.path(), DIM, Distance::Dot, false).unwrap();
    assert_eq!(storage.total_vector_count(), 2);
    assert_eq!(
        storage.get_quantized_vector(0),
        storage.get_quantized_vector(1),
    );
}
