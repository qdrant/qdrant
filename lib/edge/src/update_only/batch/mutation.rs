//! What operations do to a single point, and how a point's mutations fold
//! onto its stored form.

use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::fully_qualified_point::{FullyQualifiedPoint, StoredPoint};
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::segment_record::NamedVectorBytesOwned;
use segment::json_path::JsonPath;
use segment::types::{Payload, PayloadKeyType, PointIdType, SeqNumberType, VectorNameBuf};
use shard::operations::point_ops::UpdateMode;

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
        /// The upsert's update mode, deciding whether it applies to a point
        /// that already exists, one that does not, or both.
        mode: UpdateMode,
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
    /// Whether this mutation applies, given whether the point exists where
    /// this mutation sits in the fold. Only a conditional upsert can answer
    /// `false` — every other mutation applies unconditionally.
    ///
    /// Decided from `exists` alone, never from the point's content: an upsert
    /// whose condition is more than existence never reaches a mutation.
    fn applies(&self, exists: bool) -> bool {
        match self {
            Self::Replace {
                mode,
                vectors: _,
                payload: _,
            } => match mode {
                UpdateMode::Upsert => true,
                UpdateMode::InsertOnly => !exists,
                UpdateMode::UpdateOnly => exists,
            },
            Self::Delete
            | Self::UpdateVectors(_)
            | Self::DeleteVectors(_)
            | Self::SetPayload { .. }
            | Self::OverwritePayload(_)
            | Self::DeletePayload(_)
            | Self::ClearPayload => true,
        }
    }

    /// Whether this mutation applies whatever the point's current existence.
    /// Only such a mutation may discard the mutations before it while the
    /// batch is folded: a conditional one may turn out not to apply, and the
    /// mutations it would have superseded then have to still be there.
    fn always_applies(&self) -> bool {
        self.applies(true) && self.applies(false)
    }

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
        if mutation.always_applies() && mutation.discards_stored_point() {
            self.mutations.clear();
        }
        self.version = self.version.max(version);
        self.mutations.push(mutation);
    }

    /// Version the rewritten point is stored at.
    pub fn version(&self) -> SeqNumberType {
        self.version
    }

    /// Whether any mutation applies at all, given whether a segment holds the
    /// point today. `false` means every operation naming it was rejected by
    /// its update mode — an `insert_only` upsert of a point that is already
    /// there, or an `update_only` upsert of one that is not — and the point
    /// must be left exactly as it stands.
    ///
    /// Answering against the point's existence *before* the batch is enough:
    /// for existence to flip mid-fold some mutation has to have applied, which
    /// is what this asks in the first place.
    ///
    /// [`materialize`](Self::materialize) assumes this is `true`; call it on a
    /// point this rejects and it rewrites the point unchanged into a fresh
    /// slot.
    pub fn applies_any(&self, exists: bool) -> bool {
        self.mutations
            .iter()
            .any(|mutation| mutation.applies(exists))
    }

    /// Whether folding these mutations reads the point as it is stored today.
    /// False exactly when the first mutation that applies replaces or removes
    /// the point, and when none applies at all.
    ///
    /// Answers for a point some segment holds — one that does not exist has
    /// nothing to read either way. So an `insert_only` upsert of an existing
    /// point answers `false`: it is dropped, and the point it would have
    /// overwritten is never fetched.
    pub fn needs_stored_point(&self) -> bool {
        self.mutations
            .iter()
            .find(|mutation| mutation.applies(true))
            .is_some_and(|mutation| !mutation.discards_stored_point())
    }

    /// Fold the mutations onto `stored` — the point as it stands — into the
    /// point to store. `exists` says whether a segment holds the point;
    /// `stored` carries its content, and is absent both when the point does
    /// not exist and when [`needs_stored_point`](Self::needs_stored_point)
    /// said the fold would discard it unread.
    ///
    /// `Ok(None)` means the batch leaves nothing to store: the point ends up
    /// deleted, or an operation that can only modify an existing point named
    /// one that does not exist.
    ///
    /// Only meaningful for a point [`applies_any`](Self::applies_any) accepts.
    pub fn materialize(
        self,
        id: PointIdType,
        mut exists: bool,
        stored: Option<StoredPoint>,
    ) -> OperationResult<Option<FullyQualifiedPoint>> {
        let Self { version, mutations } = self;

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
            // An upsert whose update mode does not match the point as it
            // stands here in the fold leaves everything before it in place —
            // including a point an earlier mutation of this very batch
            // created, which is what an `insert_only` upsert has to see.
            if !mutation.applies(exists) {
                continue;
            }

            match mutation {
                PointMutation::Replace {
                    vectors,
                    payload: replacement,
                    mode: _,
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
