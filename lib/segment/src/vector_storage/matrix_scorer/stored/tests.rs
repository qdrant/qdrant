//! Stored records against the matrix scorer of the same vectors: the reference scores raw
//! records bitwise as the CPU kernels score the vectors.

use common::ambient;
use common::types::PointOffsetType;
use quantization::turboquant::TQBits;
use rand::SeedableRng as _;
use rand::rngs::StdRng;
use rand::seq::SliceRandom as _;
use rstest::rstest;

use super::super::tests::{Element, Fixture, POINTS, binary, scalar, turbo};
use crate::types::{BinaryQuantizationEncoding, Distance, QuantizationConfig, TurboQuantBitSize};
use crate::vector_storage::VectorStorage as _;
use crate::vector_storage::matrix_scorer::{
    MatrixCols, MatrixTopK, StoredRecords, new_matrix_scorer, reference, stored_records,
};

fn records(fixture: &Fixture) -> StoredRecords<'_> {
    stored_records(&fixture.storage, fixture.quantized.as_ref()).expect("has a stored form")
}

/// The record of each position of `ids`, each delivered once.
fn read_records(records: &StoredRecords, ids: &[PointOffsetType]) -> Vec<Vec<u8>> {
    let mut read: Vec<Option<Vec<u8>>> = vec![None; ids.len()];
    records
        .for_each_record(ids, |i, record| {
            assert!(read[i].is_none(), "position {i} delivered twice");
            read[i] = Some(record.to_vec());
        })
        .unwrap();
    read.into_iter()
        .enumerate()
        .map(|(i, record)| record.unwrap_or_else(|| panic!("position {i} not delivered")))
        .collect()
}

/// The reference scores raw records, read as stored and described by their form alone, bitwise
/// as the matrix kernels score the same vectors: f32 originals, and scalar quantization, whose
/// 100 dimensions are padded to 112 codes.
#[rstest]
fn reference_scores_as_the_kernels(
    #[values(None, Some(scalar()))] quantization: Option<QuantizationConfig>,
    #[values(Distance::Dot, Distance::Cosine, Distance::Euclid)] distance: Distance,
) {
    let _scope = ambient::test_guard();
    let fixture = Fixture::new(Element::F32, distance, quantization);
    let mut ids: Vec<PointOffsetType> = (0..POINTS as PointOffsetType).collect();
    ids.shuffle(&mut StdRng::seed_from_u64(3));
    let (rows, cols) = ids[..40].split_at(20);

    let records = records(&fixture);
    let (raw_rows, raw_cols) = (read_records(&records, rows), read_records(&records, cols));
    let scorer = new_matrix_scorer(&fixture.storage, fixture.quantized.as_ref()).unwrap();
    let mut top = MatrixTopK::new(cols.len());
    scorer
        .top_k(rows, MatrixCols::Other(cols), &mut top)
        .unwrap();
    for (r, row) in raw_rows.iter().enumerate() {
        let (picked, scores) = top.row(r);
        assert_eq!(picked.len(), cols.len());
        for (&c, &want) in picked.iter().zip(scores) {
            let got = reference::score_records(records.form(), row, &raw_cols[c as usize]);
            assert_eq!(
                got.to_bits(),
                want.to_bits(),
                "row {r}, col {c}: {got} vs {want}"
            );
        }
    }
}

/// `len()` counts every record, also of a deleted point, and the last id is readable.
#[rstest]
fn len_counts_every_record(
    #[values(None, Some(scalar()))] quantization: Option<QuantizationConfig>,
) {
    let _scope = ambient::test_guard();
    let mut fixture = Fixture::new(Element::F32, Distance::Dot, quantization);
    let last = POINTS as PointOffsetType - 1;
    assert!(fixture.storage.delete_vector(last).unwrap());
    let records = records(&fixture);
    assert_eq!(records.len(), POINTS);
    assert!(!records.is_empty());
    let read = read_records(&records, &[last]);
    assert_eq!(read[0].len(), records.form().record_bytes());
}

/// Formats without a stored form yet. Quantized vectors without one never fall back to the
/// originals, which have one here.
#[test]
fn no_stored_form() {
    let _scope = ambient::test_guard();
    let none =
        |fixture: &Fixture| stored_records(&fixture.storage, fixture.quantized.as_ref()).is_none();
    assert!(none(&Fixture::new(Element::F32, Distance::Manhattan, None)));
    assert!(none(&Fixture::new(
        Element::F32,
        Distance::Manhattan,
        Some(scalar())
    )));
    for element in [Element::U8, Element::F16, Element::Turbo(TQBits::Bits4)] {
        assert!(
            none(&Fixture::new(element, Distance::Dot, None)),
            "{element:?}"
        );
    }
    for quantization in [
        binary(BinaryQuantizationEncoding::OneBit),
        turbo(TurboQuantBitSize::Bits4),
    ] {
        let fixture = Fixture::new(Element::F32, Distance::Dot, Some(quantization));
        assert!(none(&fixture));
        assert!(new_matrix_scorer(&fixture.storage, fixture.quantized.as_ref()).is_some());
        assert!(stored_records(&fixture.storage, None).is_some());
    }
}
