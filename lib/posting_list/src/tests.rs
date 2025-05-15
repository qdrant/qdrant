use std::ops::Deref;

use common::types::PointOffsetType;

use crate::value_handler::VarSized;
use crate::{CHUNK_SIZE, PostingBuilder, PostingList, VarSizedValue};

// Simple struct that implements VarSizedValue for testing
#[derive(Debug, Clone, PartialEq)]
struct TestString(String);

impl VarSizedValue for TestString {
    fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(4 + self.0.len());
        // Store length as u32
        let len = self.0.len() as u32;
        result.extend_from_slice(&len.to_le_bytes());
        // Store actual string bytes
        result.extend_from_slice(self.0.as_bytes());
        result
    }

    fn from_bytes(data: &[u8]) -> Self {
        // Extract length
        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(&data[0..4]);
        let len = u32::from_le_bytes(len_bytes) as usize;

        // Extract string
        let s = String::from_utf8_lossy(&data[4..4 + len]).to_string();
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
        test_data.push((i, TestString(format!("item_{}", i))));
    }

    // Verify we have more than CHUNK_SIZE elements
    assert!(
        test_data.len() > CHUNK_SIZE,
        "Test data should have more than 128 elements but has {}",
        test_data.len()
    );

    // Build our reference model - convert all string slices to owned strings
    let reference_model: Vec<(PointOffsetType, TestString)> = test_data
        .iter()
        .map(|(id, s)| (*id, TestString(s.to_string())))
        .collect();

    // Create the posting list builder and add elements
    let mut builder = PostingBuilder::new();
    for (id, value) in &reference_model {
        builder.add(*id, value.clone());
    }

    // Build the actual posting list
    let posting_list = builder.build_var_sized();

    // Access the posting list
    let mut visitor = posting_list.visitor();

    // Validate len()
    assert_eq!(visitor.len(), reference_model.len());

    // Iterate through the elements in reference_model and check they can be found
    for (offset, (expected_id, expected_value)) in reference_model.iter().enumerate() {
        let Some(elem) = visitor.get_by_offset(offset) else {
            panic!("Element not found at offset {}", offset);
        };

        assert_eq!(elem.id, *expected_id);
        assert_eq!(&elem.value, expected_value);
    }
}
