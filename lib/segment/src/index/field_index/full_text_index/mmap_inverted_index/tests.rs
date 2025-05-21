use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use rand::Rng;

use super::{mmap_postings, old_mmap_postings};
use crate::index::field_index::full_text_index::compressed_posting::compressed_posting_list::CompressedPostingList;

fn generate_ids(rng: &mut impl Rng, amount: usize) -> Vec<PointOffsetType> {
    let distr = rand::distr::Uniform::new(0, amount as u32).unwrap();
    rng.sample_iter(distr).take(amount).collect()
}

#[test]
fn test_mmap_posting_lists_compatibility() {
    let rng = &mut rand::rng();
    let lengths = [138, 14, 1889, 128, 129, 127];

    // postings in a vec of vecs
    let postings = lengths
        .into_iter()
        .map(|len| generate_ids(rng, len))
        .collect::<Vec<_>>();

    // old compressed postings implementation
    let old_compressed_postings = postings
        .iter()
        .map(|ids| CompressedPostingList::new(ids))
        .collect::<Vec<_>>();

    let dir = tempfile::tempdir().unwrap();
    let postings_path = dir.path().join("postings.dat");

    // Create mmap postings file
    old_mmap_postings::MmapPostings::create(postings_path.clone(), &old_compressed_postings)
        .unwrap();

    // open with old impl
    let old_postings = old_mmap_postings::MmapPostings::open(postings_path.clone(), true).unwrap();

    // open with new impl
    let new_postings = mmap_postings::MmapPostings::open(postings_path.clone(), true).unwrap();

    let hw_counter = HardwareCounterCell::disposable();

    for token_id in 0..postings.len() as u32 {
        let old = old_postings.get(token_id, &hw_counter).unwrap();
        let new = new_postings.get(token_id, &hw_counter).unwrap();
        let model = &postings[token_id as usize];

        // check all impls iterate the same ids
        for (offset, id_old, elem_new, &id_model) in
            itertools::multizip((0u32.., old.iter(), new.into_iter(), model.iter()))
        {
            assert!(
                id_model == id_old && id_model == elem_new.id,
                "Mismatch at token_id {token_id}: offset: {offset}, old: {id_old}, new: {}, model: {id_model}",
                elem_new.id
            );
        }
    }
}
