use common::types::PointOffsetType;
use rand::distr::{Alphanumeric, SampleString};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::value_handler::ValueHandler;
use crate::{CHUNK_LEN, PostingBuilder, PostingList, UnsizedValue};

// Simple struct that implements VarSizedValue for testing
#[derive(Debug, Clone, PartialEq)]
struct TestString(String);

impl UnsizedValue for TestString {
    fn write_len(&self) -> usize {
        self.0.len()
    }

    fn write_to(&self, dst: &mut [u8]) {
        dst.copy_from_slice(self.0.as_bytes());
    }

    fn from_bytes(data: &[u8]) -> Self {
        let s = String::from_utf8(data.to_vec()).expect("Failed to convert bytes to string");
        TestString(s)
    }
}

#[test]
fn test_just_ids_against_vec() {
    check_various_lengths(|len| {
        let posting_list = check_against_sorted_vec(|_rng, _id| (), |builder| builder.build(), len);

        // validate that chunks' sized values are empty
        if let Some(first_chunk) = posting_list.chunks.first() {
            let chunks_size = size_of_val(first_chunk);
            let expected_chunk_size = size_of::<u32>() * 2;
            assert_eq!(chunks_size, expected_chunk_size);
        }

        // validate var_sized_data is empty
        assert_eq!(posting_list.var_size_data.len(), 0);
    })
}

#[test]
fn test_var_sized_against_vec() {
    let alphanumeric = Alphanumeric;
    check_various_lengths(|len| {
        check_against_sorted_vec(
            |rng, id| {
                let len = rng.random_range(1..=20);
                let s = alphanumeric.sample_string(rng, len);
                TestString(format!("item_{id} {s}"))
            },
            |builder| builder.build_unsized(),
            len,
        );
    })
}

#[test]
fn test_fixed_sized_against_vec() {
    check_various_lengths(|len| {
        check_against_sorted_vec(
            |_rng, id| u64::from(id) * 100,
            |builder| builder.build_sized(),
            len,
        );
    });
}

fn generate_data<T, R: Rng>(
    amount: u32,
    rng: &mut R,
    gen_value: impl Fn(&mut R, u32) -> T,
) -> Vec<(u32, T)> {
    let gen_id = |rng: &mut R| rng.random_range(0..amount);

    (0..amount)
        .map(|_| {
            let id = gen_id(rng);
            (id, gen_value(rng, id))
        })
        .collect()
}

fn check_various_lengths(check: impl Fn(u32)) {
    let lengths = [
        0,
        1,
        2,
        9,
        10,
        CHUNK_LEN - 1,
        CHUNK_LEN,
        CHUNK_LEN + 1,
        100 * CHUNK_LEN,
        500 * CHUNK_LEN + 1,
        500 * CHUNK_LEN - 1,
        500 * CHUNK_LEN + CHUNK_LEN / 2,
    ];
    for len in lengths {
        check(len as u32);
    }
}

fn check_against_sorted_vec<G, H, B>(gen_value: G, build: B, postings_count: u32) -> PostingList<H>
where
    G: Fn(&mut StdRng, PointOffsetType) -> H::Value,
    H: ValueHandler,
    B: FnOnce(PostingBuilder<H::Value>) -> PostingList<H>,
    H::Value: Clone + PartialEq,
{
    let rng = &mut StdRng::seed_from_u64(42);
    let test_data = generate_data(postings_count, rng, gen_value);

    // Build our reference model
    let mut model = test_data.clone();
    model.sort_unstable_by_key(|(id, _)| *id);

    // Create the posting list builder and add elements
    let mut builder = PostingBuilder::new();
    for (id, value) in test_data {
        builder.add(id, value);
    }

    // Build the actual posting list
    let posting_list = build(builder);

    // Access the posting list
    let mut visitor = posting_list.visitor();

    // Validate len()
    assert_eq!(visitor.len(), model.len());

    // Iterate through the elements in reference_model and check they can be found
    for (offset, (expected_id, expected_value)) in model.iter().enumerate() {
        let Some(elem) = visitor.get_by_offset(offset) else {
            panic!("Element not found at offset {offset}");
        };

        assert_eq!(elem.id, *expected_id);
        assert_eq!(elem.value, *expected_value);

        // also check that contains function works
        assert!(visitor.contains(*expected_id));
    }

    // Bounds check
    assert!(visitor.get_by_offset(postings_count as usize).is_none());
    let out_of_range = (postings_count.next_multiple_of(CHUNK_LEN as u32)) as usize;
    assert!(visitor.get_by_offset(out_of_range).is_none());

    // There is no such id
    assert!(!visitor.contains(postings_count));

    posting_list
}
