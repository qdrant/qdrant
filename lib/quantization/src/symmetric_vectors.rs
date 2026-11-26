//! Stored codes as plain vectors: the geometry their symmetric score
//! ([`EncodedVectors::score_internal`](crate::EncodedVectors::score_internal)) scores.

/// Stored codes as the vectors their symmetric score scores.
pub trait SymmetricVectors {
    /// Length of [`Self::symmetric_vector`].
    fn symmetric_dim(&self) -> usize;

    /// `code`, a stored vector, as a vector of [`Self::symmetric_dim`] values whose dot product,
    /// or distance, with another such vector is, up to an affine map, the symmetric score of the
    /// two codes. Exactly, unless the implementation says otherwise.
    fn symmetric_vector<'a>(&'a self, code: &'a [u8]) -> impl Iterator<Item = f32> + 'a;

    /// What makes `code` a duplicate of other codes to the symmetric score.
    fn duplicate_key<'c>(&self, code: &'c [u8]) -> DuplicateKey<'c>;
}

/// What makes stored vectors, original or quantized, duplicates to their score: vectors with equal
/// keys score alike against every vector, up to rounding.
pub enum DuplicateKey<'a> {
    /// Its stored bytes: the code of a quantized vector.
    Bytes(&'a [u8]),
    /// Its values: an original vector as stored, or a code as its
    /// [`SymmetricVectors::symmetric_vector`], where the code also stores values the score
    /// ignores, so codes that differ only there are alike.
    Values,
    /// None: the score of the vector against itself is NaN, so it duplicates no vector.
    ///
    /// Only reported where finite input can encode to such a code: TurboQuant, whose stored scale
    /// or correction a NaN in the encoded vector makes NaN. Scalar and binary codes are taken to
    /// score finite, also where extreme values overflow their score.
    ScoresNan,
}
