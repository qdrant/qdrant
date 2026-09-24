use std::collections::HashMap;

use common::types::PointOffsetType;
use rand::distr::{Alphanumeric, SampleString};
use rand::rngs::StdRng;
use rand::{Rng, RngExt, SeedableRng};

use crate::{CHUNK_LEN, PostingBuilder, PostingList, PostingValue, UnsizedHandler, UnsizedValue};

// Simple struct that implements VarSizedValue for testing
#[derive(Debug, Clone, PartialEq)]
struct TestString(String);

impl PostingValue for TestString {
    type Handler = UnsizedHandler<TestString>;
}
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
        let posting_list = check_against_sorted_vec(|_rng, _id| (), len);

        // validate that chunks' sized values are empty, so we only have initial_id and offset
        if let Some(chunk) = posting_list.chunks.first() {
            assert_eq!(size_of_val(chunk), size_of::<u32>() * 2);
        }

        // similarly, validate that the remainder is equivalent to just one id
        if let Some(remainder) = posting_list.remainders.first() {
            assert_eq!(size_of_val(remainder), size_of::<u32>());
        }
    });
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
            len,
        );
    })
}

/// The length cursor yields the same ids as the value iterator, and a length
/// equal to what the value would serialize to, for both value kinds and for
/// lists that span chunks and remainders. Seeking lands on the same element as
/// the value iterator's seek.
#[test]
fn test_len_iter_matches_values() {
    fn check<V, G>(gen_value: G, expected_len: impl Fn(&V) -> usize)
    where
        V: PostingValue + PartialEq + std::fmt::Debug,
        G: Fn(&mut StdRng, PointOffsetType) -> V,
    {
        check_various_lengths(|len| {
            let rng = &mut StdRng::seed_from_u64(7);
            let mut model = generate_data(len, rng, &gen_value);
            model.sort_unstable_by_key(|(id, _)| *id);
            // Duplicate ids carry different values, and which one the builder
            // keeps is not the point here.
            model.dedup_by_key(|(id, _)| *id);

            let mut builder = PostingBuilder::new();
            for (id, value) in model.iter().cloned() {
                builder.add(id, value);
            }
            let posting_list = builder.build();

            let lens: Vec<_> = posting_list.view().len_iter().collect();
            assert_eq!(lens.len(), model.len());
            for (elem, (id, value)) in lens.iter().zip(&model) {
                assert_eq!(elem.id, *id);
                assert_eq!(elem.value_len, expected_len(value));
            }

            // Seeking: every model id, an id between two model ids, and one past the end.
            let mut len_cursor = posting_list.view().len_iter();
            let mut value_cursor = posting_list.iter();
            let targets = model
                .iter()
                .map(|(id, _)| *id)
                .flat_map(|id| [id, id + 1])
                .chain(std::iter::once(u32::MAX));
            for target in targets {
                let expected = value_cursor.advance_until_greater_or_equal(target);
                let actual = len_cursor.advance_until_greater_or_equal(target);
                assert_eq!(actual.map(|e| e.id), expected.as_ref().map(|e| e.id));
                assert_eq!(
                    actual.map(|e| e.value_len),
                    expected.as_ref().map(|e| expected_len(&e.value)),
                );
                assert_eq!(len_cursor.current(), actual);
            }
        });
    }

    let alphanumeric = Alphanumeric;
    check(
        |rng, id| {
            let len = rng.random_range(0..=20);
            TestString(format!("{id}{}", alphanumeric.sample_string(rng, len)))
        },
        |value: &TestString| value.write_len(),
    );
    check(
        |_rng, id| u64::from(id) * 3,
        |_value: &u64| size_of::<u64>(),
    );
    check(|_rng, _id| (), |_value: &()| 0);
}

#[test]
fn test_fixed_sized_against_vec() {
    check_various_lengths(|len| {
        check_against_sorted_vec(|_rng, id| u64::from(id) * 100, len);
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
        CHUNK_LEN + 2,
        2 * CHUNK_LEN + 10,
        100 * CHUNK_LEN,
        500 * CHUNK_LEN + 1,
        500 * CHUNK_LEN - 1,
        500 * CHUNK_LEN + CHUNK_LEN / 2,
    ];
    for len in lengths {
        check(len as u32);
    }
}

fn check_against_sorted_vec<G, V>(gen_value: G, postings_count: u32) -> PostingList<V>
where
    G: Fn(&mut StdRng, PointOffsetType) -> V,
    V: PostingValue + PartialEq + std::fmt::Debug,
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
    let posting_list = builder.build();

    // Access the posting list
    let mut visitor = posting_list.visitor();
    let mut intersection_iter = posting_list.iter();

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

        // also check that the intersection is full
        let intersection = intersection_iter
            .advance_until_greater_or_equal(*expected_id)
            .unwrap();
        assert_eq!(intersection.id, *expected_id);
    }

    // Bounds check
    assert!(visitor.get_by_offset(postings_count as usize).is_none());
    let out_of_range = postings_count.next_multiple_of(CHUNK_LEN as u32) as usize;
    assert!(visitor.get_by_offset(out_of_range).is_none());

    // There is no such id
    assert!(!visitor.contains(postings_count));

    // intersect against all sequential ids in the posting range, model is a hashmap in this case
    let model = model.into_iter().collect::<HashMap<_, _>>();
    let mut intersection_iter = posting_list.iter();
    for seq_id in 0..postings_count {
        let model_contains = model.contains_key(&seq_id);
        let iter_contains = intersection_iter
            .advance_until_greater_or_equal(seq_id)
            .is_some_and(|elem| elem.id == seq_id);
        assert_eq!(model_contains, iter_contains, "Mismatch at seq_id {seq_id}");
    }

    posting_list
}
