use serde::{Deserialize, Serialize};

/// Distance type used by TurboQuant.
///
/// Duplicated from `quantization::encoded_vectors::DistanceType` because this
/// crate must not depend on `quantization` (the dependency points the other
/// way). Unlike the quantization-crate version, TQ always distinguishes
/// `Cosine` from `Dot` — the legacy conflation that older quantization
/// storages carry over does not apply here, since TQ was introduced after
/// the two were split.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceType {
    Cosine,
    Dot,
    L1,
    L2,
}
