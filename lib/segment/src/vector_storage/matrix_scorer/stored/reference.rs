//! Scores two raw records from their [`StoredForm`] alone, as the CPU kernels score their decoded
//! vectors: the reference that other implementations, such as shaders, are tested against.

use pairwise::KernelElem;

use super::{StoredForm, StoredFormat};

/// The score of records `a` and `b`, each [`StoredForm::record_bytes`] long: `a` is the row's
/// record and `b` the column's, as the CPU kernels order the values they combine.
pub(crate) fn score_records(form: &StoredForm, a: &[u8], b: &[u8]) -> f32 {
    assert_eq!(a.len(), form.record_bytes());
    assert_eq!(b.len(), form.record_bytes());
    let (elements_a, elements_b) = (Elements::of(form, a), Elements::of(form, b));
    let dot = match (&elements_a, &elements_b) {
        (Elements::F32(a), Elements::F32(b)) => f32::dot(a, b, ()),
        (Elements::U8(a), Elements::U8(b)) => u8::dot(a, b, ()),
        (Elements::F32(_), Elements::U8(_)) | (Elements::U8(_), Elements::F32(_)) => {
            unreachable!("both records have the same form")
        }
    };
    let aux_a = [form.value_of(a), 0.0, 0.0];
    let aux_b = [form.value_of(b), 0.0, 0.0];
    form.combine().score(dot, &aux_a, &aux_b)
}

/// The elements of a record.
enum Elements {
    F32(Vec<f32>),
    U8(Vec<u8>),
}

impl Elements {
    fn of(form: &StoredForm, record: &[u8]) -> Self {
        let start = form.elements_at();
        match form.format() {
            StoredFormat::F32 => {
                let bytes = &record[start..start + form.depth() * size_of::<f32>()];
                // Native byte order, read unaligned
                Elements::F32(bytemuck::pod_collect_to_vec(bytes))
            }
            StoredFormat::Sq => Elements::U8(record[start..start + form.depth()].to_vec()),
        }
    }
}
