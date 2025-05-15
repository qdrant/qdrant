use std::borrow::Cow;
use std::ops::Deref;

use common::types::PointOffsetType;

use crate::{CHUNK_SIZE, PostingBuilder, VarSizedValue};

// Simple struct that implements VarSizedValue for testing
#[derive(Debug, Clone, PartialEq)]
struct TestString(String);

impl VarSizedValue for TestString {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(self.as_bytes())
    }

    fn from_bytes(data: &[u8]) -> Self {
        let s = String::from_utf8_lossy(data).to_string();
        TestString(s)
    }
}

impl Deref for TestString {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Tests one posting list against a Vec<(u32, T)> implementation
#[test]
fn model_test_var_sized() {
    // Create initial test data with named values
    let mut test_data = vec![
        (1u32, "first"),
        (5u32, "fifth"),
        (10u32, "tenth"),
        (15u32, "fifteenth"),
        (20u32, "twentieth"),
        (25u32, "twenty-fifth"),
        (30u32, "thirtieth"),
        (100u32, "hundredth"),
        (101u32, "hundred and first"),
        (200u32, "two hundredth"),
        (300u32, "three hundredth"),
        (400u32, "four hundredth"),
        (500u32, "five hundredth"),
    ]
    .into_iter()
    .map(|(id, s)| (id, TestString(s.to_string())))
    .collect::<Vec<_>>();

    // Add additional elements to fill more than a CHUNK_SIZE
    for i in 1000..1300 {
        test_data.push((i, TestString(format!("item_{i}"))));
    }

    // Verify we have more than CHUNK_SIZE elements
    assert!(
        test_data.len() > CHUNK_SIZE,
        "Test data should have more than 128 elements but has {}",
        test_data.len()
    );

    // Build our reference model - convert all string slices to owned strings
    let vec: Vec<(PointOffsetType, TestString)> = test_data
        .iter()
        .map(|(id, s)| (*id, TestString(s.to_string())))
        .collect();

    // Create the posting list builder and add elements
    let mut builder = PostingBuilder::new();
    for (id, value) in &vec {
        builder.add(*id, value.clone());
    }

    // Build the actual posting list
    let posting_list = builder.build_var_sized();

    // Access the posting list
    let mut visitor = posting_list.visitor();

    // Validate len()
    assert_eq!(visitor.len(), vec.len());

    // Iterate through the elements in reference_model and check they can be found
    for (offset, (expected_id, expected_value)) in vec.iter().enumerate() {
        let Some(elem) = visitor.get_by_offset(offset) else {
            panic!("Element not found at offset {offset}");
        };

        assert_eq!(elem.id, *expected_id);
        assert_eq!(&elem.value, expected_value);

        // also check that contains function works
        assert!(visitor.contains(*expected_id));
    }
}

#[test]
fn model_test_sized() {
    // Create initial test data with named values
    let mut test_data = vec![
        (1u32, 100u64),
        (5u32, 500u64),
        (10u32, 1000u64),
        (15u32, 1500u64),
        (20u32, 2000u64),
        (25u32, 2500u64),
        (30u32, 3000u64),
        (100u32, 10000u64),
        (101u32, 10100u64),
        (200u32, 20000u64),
        (300u32, 30000u64),
        (400u32, 40000u64),
        (500u32, 50000u64),
    ];

    // Add additional elements to fill more than a CHUNK_SIZE
    for i in 1000..1300 {
        test_data.push((i, u64::from(i) * 100));
    }

    // Verify we have more than CHUNK_SIZE elements
    assert!(
        test_data.len() > CHUNK_SIZE,
        "Test data should have more than 128 elements but has {}",
        test_data.len()
    );

    // Build our reference model
    let vec: Vec<(PointOffsetType, u64)> = test_data.clone();

    // Create the posting list builder and add elements
    let mut builder = PostingBuilder::new();
    for (id, value) in &vec {
        builder.add(*id, *value);
    }

    // Build the actual posting list
    let posting_list = builder.build_sized();

    // Access the posting list
    let mut visitor = posting_list.visitor();

    // Validate len()
    assert_eq!(visitor.len(), vec.len());

    // Iterate through the elements in reference_model and check they can be found
    for (offset, (expected_id, expected_value)) in vec.iter().enumerate() {
        let Some(elem) = visitor.get_by_offset(offset) else {
            panic!("Element not found at offset {offset}");
        };

        assert_eq!(elem.id, *expected_id);
        assert_eq!(elem.value, *expected_value);

        // also check that contains function works
        assert!(visitor.contains(*expected_id));
    }
}
