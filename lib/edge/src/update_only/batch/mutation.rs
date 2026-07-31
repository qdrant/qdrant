//! What operations do to a single point, and how a point's mutations fold
//! onto its stored form.

use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::fully_qualified_point::{FullyQualifiedPoint, StoredPoint};
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::segment_record::NamedVectorBytesOwned;
use segment::json_path::JsonPath;
use segment::types::{Payload, PayloadKeyType, PointIdType, SeqNumberType, VectorNameBuf};

/// The vectors an operation carries, in the form the operation carried them:
/// storage-native bytes travel to the new slot untouched, decoded vectors are
/// encoded by the storage. Keeping the two apart avoids a decode/re-encode
/// round-trip a quantized storage would not survive losslessly.
pub enum OperationVectors {
    Decoded(NamedVectors<'static>),
    Raw(NamedVectorBytesOwned),
}

/// What a single operation does to a single point; one variant per accepted
/// operation.
pub enum PointMutation {
    /// Whole-point replacement (an upsert): both vectors and payload come from
    /// the operation, and nothing of a previously stored point survives.
    Replace {
        vectors: OperationVectors,
        payload: Payload,
    },
    /// The point is removed.
    Delete,
    /// Replace the named vectors, leaving the rest of the point alone.
    UpdateVectors(NamedVectors<'static>),
    /// Drop the named vectors, leaving the rest of the point alone.
    DeleteVectors(Vec<VectorNameBuf>),
    /// Merge into the stored payload, at `key` when given.
    SetPayload {
        payload: Payload,
        key: Option<JsonPath>,
    },
    /// Replace the whole payload.
    OverwritePayload(Payload),
    /// Drop the listed payload keys.
    DeletePayload(Vec<PayloadKeyType>),
    /// Drop the whole payload.
    ClearPayload,
}

impl PointMutation {
    /// Whether this mutation makes every mutation before it irrelevant:
    /// nothing of the point as it stood survives, so neither the earlier
    /// mutations nor the stored point itself need to be looked at.
    fn discards_stored_point(&self) -> bool {
        match self {
            Self::Replace { .. } | Self::Delete => true,
            Self::UpdateVectors(_)
            | Self::DeleteVectors(_)
            | Self::SetPayload { .. }
            | Self::OverwritePayload(_)
            | Self::DeletePayload(_)
            | Self::ClearPayload => false,
        }
    }
}

/// Everything a batch does to one point, in operation order.
pub struct PointUpdates {
    /// Operation number of the last operation folded in — the version the
    /// rewritten point is stored at.
    version: SeqNumberType,
    /// Mutations to fold onto the stored point, oldest first. Never empty.
    mutations: Vec<PointMutation>,
}

impl PointUpdates {
    pub(super) fn new(version: SeqNumberType, mutation: PointMutation) -> Self {
        Self {
            version,
            mutations: vec![mutation],
        }
    }

    pub(super) fn push(&mut self, version: SeqNumberType, mutation: PointMutation) {
        if mutation.discards_stored_point() {
            self.mutations.clear();
        }
        self.version = self.version.max(version);
        self.mutations.push(mutation);
    }

    /// Version the rewritten point is stored at.
    pub fn version(&self) -> SeqNumberType {
        self.version
    }

    /// Whether applying these mutations requires reading the point as it is
    /// stored today. False exactly when the first surviving mutation replaces
    /// or removes the point.
    pub fn needs_stored_point(&self) -> bool {
        self.mutations
            .first()
            .is_none_or(|mutation| !mutation.discards_stored_point())
    }

    /// Fold the mutations onto `stored` — the point as it stands, absent when
    /// no segment holds it — into the point to store.
    ///
    /// `Ok(None)` means the batch leaves nothing to store: the point ends up
    /// deleted, or an operation that can only modify an existing point named
    /// one that does not exist.
    pub fn materialize(
        self,
        id: PointIdType,
        stored: Option<StoredPoint>,
    ) -> OperationResult<Option<FullyQualifiedPoint>> {
        let Self { version, mutations } = self;

        let mut exists = stored.is_some();
        let (mut stored_vectors, mut payload) = match stored {
            Some(stored) => {
                let StoredPoint {
                    internal_id: _,
                    vectors,
                    payload,
                } = stored;
                (vectors, payload)
            }
            None => (NamedVectorBytesOwned::new(), Payload::default()),
        };
        // Vectors the batch supplied, which override `stored_vectors` by name
        // (see `FullyQualifiedPoint`), so replacing one does not require
        // removing its carried-over counterpart.
        let mut updated_vectors = NamedVectors::default();

        for mutation in mutations {
            match mutation {
                PointMutation::Replace {
                    vectors,
                    payload: replacement,
                } => {
                    exists = true;
                    stored_vectors.clear();
                    updated_vectors = NamedVectors::default();
                    match vectors {
                        OperationVectors::Decoded(vectors) => updated_vectors = vectors,
                        OperationVectors::Raw(vectors) => stored_vectors = vectors,
                    }
                    payload = replacement;
                }
                PointMutation::Delete => {
                    exists = false;
                    stored_vectors.clear();
                    updated_vectors = NamedVectors::default();
                    payload = Payload::default();
                }
                PointMutation::UpdateVectors(vectors) => {
                    if !exists {
                        return Err(OperationError::PointIdError {
                            missed_point_id: id,
                        });
                    }
                    updated_vectors.merge(vectors);
                }
                PointMutation::DeleteVectors(names) => {
                    for name in &names {
                        stored_vectors.retain(|(stored_name, _)| stored_name != name);
                        updated_vectors.remove_ref(name.as_str());
                    }
                }
                PointMutation::SetPayload {
                    payload: values,
                    key,
                } => match key {
                    Some(key) => payload.merge_by_key(&values, &key),
                    None => payload.merge(&values),
                },
                PointMutation::OverwritePayload(values) => payload = values,
                PointMutation::DeletePayload(keys) => {
                    for key in &keys {
                        payload.remove(key);
                    }
                }
                PointMutation::ClearPayload => payload = Payload::default(),
            }
        }

        if !exists {
            return Ok(None);
        }

        Ok(Some(FullyQualifiedPoint {
            id,
            version,
            stored_vectors,
            updated_vectors,
            payload,
        }))
    }
}
