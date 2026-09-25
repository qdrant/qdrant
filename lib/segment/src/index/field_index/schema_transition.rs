//! Classify a payload index schema transition between the previously stored
//! schema and the newly requested one.
//!
//! Used by `StructPayloadIndex::set_indexed` to detect in-place updates that
//! do not require rebuilding the field index from payload storage:
//!
//! - `on_disk` flip on non-appendable segments: reload existing files in the
//!   new storage mode.
//! - `enable_hnsw` flip: update persisted schema only; index files are unchanged.
//!
//! Each per-kind arm normalizes the known in-place fields on a clone and
//! compares the rest via the derived `PartialEq`, so a newly added field is
//! accounted for automatically: any difference outside those fields yields
//! `Incompatible`.

// Deprecated storage placement params (`on_disk`, `always_ram`, `on_disk_payload`) are still
// handled here for backward compatibility with the new `memory` parameter
#![allow(deprecated)]

use crate::types::{PayloadFieldSchema, PayloadSchemaParams};

/// Compatible in-place schema diff. Fields are independent so combinations
/// (e.g. `on_disk` + `enable_hnsw`) need no extra enum variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompatibleDiff {
    /// `Some(new_on_disk)` if the `on_disk` flag flipped; `None` if unchanged.
    pub on_disk: Option<bool>,
    /// True if `enable_hnsw` (persisted `Option<bool>`) changed.
    pub metadata: bool,
}

impl CompatibleDiff {
    /// True when only schema-metadata flags changed (no storage placement flip).
    pub fn metadata_only(self) -> bool {
        self.on_disk.is_none() && self.metadata
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaTransition {
    /// The two schemas are functionally identical (modulo `FieldType` vs
    /// fully-expanded `FieldParams`).
    Identical,
    /// The two schemas differ only in in-place-updatable flags (`on_disk`
    /// and/or `enable_hnsw`). See [`CompatibleDiff`].
    Compatible(CompatibleDiff),
    /// The two schemas differ in a way that requires the legacy
    /// drop-and-rebuild path.
    Incompatible,
}

pub fn classify(old: &PayloadFieldSchema, new: &PayloadFieldSchema) -> SchemaTransition {
    let old = old.expand();
    let new = new.expand();
    let old = &*old;
    let new = &*new;

    if old == new {
        return SchemaTransition::Identical;
    }

    if let Some(mut diff) = compatible_diff(old, new) {
        // Resolve through `is_on_disk()` so `memory` placement is respected.
        if diff.on_disk.is_some() {
            diff.on_disk = Some(new.is_on_disk());
        }
        return SchemaTransition::Compatible(diff);
    }

    SchemaTransition::Incompatible
}

fn compatible_diff(old: &PayloadSchemaParams, new: &PayloadSchemaParams) -> Option<CompatibleDiff> {
    use PayloadSchemaParams as P;

    // Compare `on_disk` / `enable_hnsw` at the `Option<bool>` level: `None` vs
    // `Some(false)` (or `Some(true)` for enable_hnsw) both may mean the same
    // effective value yet the persisted value differs, so it still counts as a
    // flip. Every other field goes through the derived `PartialEq` after
    // normalizing those flags on a clone — a newly added field is therefore
    // accounted for automatically (any difference makes this `None`, i.e.
    // `Incompatible`, the safe default).
    macro_rules! compatible {
        ($a:expr, $b:expr) => {{
            let on_disk_changed = $a.on_disk != $b.on_disk;
            let metadata_changed = $a.enable_hnsw != $b.enable_hnsw;
            if !on_disk_changed && !metadata_changed {
                None
            } else {
                let mut normalized = $a.clone();
                normalized.on_disk = $b.on_disk;
                normalized.enable_hnsw = $b.enable_hnsw;
                if normalized == *$b {
                    Some(CompatibleDiff {
                        // Placeholder; `classify` overwrites with `new.is_on_disk()`.
                        on_disk: on_disk_changed.then_some(false),
                        metadata: metadata_changed,
                    })
                } else {
                    None
                }
            }
        }};
    }

    match (old, new) {
        (P::Keyword(a), P::Keyword(b)) => compatible!(a, b),
        (P::Integer(a), P::Integer(b)) => compatible!(a, b),
        (P::Float(a), P::Float(b)) => compatible!(a, b),
        (P::Geo(a), P::Geo(b)) => compatible!(a, b),
        (P::Text(a), P::Text(b)) => compatible!(a, b),
        (P::Bool(a), P::Bool(b)) => compatible!(a, b),
        (P::Datetime(a), P::Datetime(b)) => compatible!(a, b),
        (P::Uuid(a), P::Uuid(b)) => compatible!(a, b),
        // Cross-kind pairs cannot be compatible in-place. Listed exhaustively
        // (rather than `_ =>`) so a new `PayloadSchemaParams` variant triggers
        // a compile error here.
        (P::Keyword(_), _)
        | (P::Integer(_), _)
        | (P::Float(_), _)
        | (P::Geo(_), _)
        | (P::Text(_), _)
        | (P::Bool(_), _)
        | (P::Datetime(_), _)
        | (P::Uuid(_), _) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_types::index::{
        BoolIndexParams, BoolIndexType, DatetimeIndexParams, DatetimeIndexType, FloatIndexParams,
        FloatIndexType, GeoIndexParams, GeoIndexType, IntegerIndexParams, IntegerIndexType,
        KeywordIndexParams, KeywordIndexType, TextIndexParams, TextIndexType, TokenizerType,
        UuidIndexParams, UuidIndexType,
    };
    use crate::types::PayloadSchemaType;

    fn wrap(p: PayloadSchemaParams) -> PayloadFieldSchema {
        PayloadFieldSchema::FieldParams(p)
    }

    fn keyword(
        on_disk: Option<bool>,
        is_tenant: Option<bool>,
        enable_hnsw: Option<bool>,
    ) -> PayloadSchemaParams {
        PayloadSchemaParams::Keyword(KeywordIndexParams {
            memory: None,
            r#type: KeywordIndexType::Keyword,
            is_tenant,
            on_disk,
            enable_hnsw,
            prefix: None,
        })
    }

    fn integer(
        on_disk: Option<bool>,
        lookup: Option<bool>,
        enable_hnsw: Option<bool>,
    ) -> PayloadSchemaParams {
        PayloadSchemaParams::Integer(IntegerIndexParams {
            memory: None,
            r#type: IntegerIndexType::Integer,
            lookup,
            range: Some(true),
            is_principal: None,
            on_disk,
            enable_hnsw,
        })
    }

    fn float(on_disk: Option<bool>, enable_hnsw: Option<bool>) -> PayloadSchemaParams {
        PayloadSchemaParams::Float(FloatIndexParams {
            memory: None,
            r#type: FloatIndexType::Float,
            is_principal: None,
            on_disk,
            enable_hnsw,
        })
    }

    fn geo(on_disk: Option<bool>, enable_hnsw: Option<bool>) -> PayloadSchemaParams {
        PayloadSchemaParams::Geo(GeoIndexParams {
            memory: None,
            r#type: GeoIndexType::Geo,
            on_disk,
            enable_hnsw,
        })
    }

    fn text(
        on_disk: Option<bool>,
        tokenizer: TokenizerType,
        enable_hnsw: Option<bool>,
    ) -> PayloadSchemaParams {
        PayloadSchemaParams::Text(TextIndexParams {
            memory: None,
            r#type: TextIndexType::Text,
            tokenizer,
            min_token_len: None,
            max_token_len: None,
            lowercase: None,
            ascii_folding: None,
            phrase_matching: None,
            stopwords: None,
            on_disk,
            stemmer: None,
            enable_hnsw,
        })
    }

    fn bool_p(on_disk: Option<bool>, enable_hnsw: Option<bool>) -> PayloadSchemaParams {
        PayloadSchemaParams::Bool(BoolIndexParams {
            memory: None,
            r#type: BoolIndexType::Bool,
            on_disk,
            enable_hnsw,
        })
    }

    fn datetime(
        on_disk: Option<bool>,
        is_principal: Option<bool>,
        enable_hnsw: Option<bool>,
    ) -> PayloadSchemaParams {
        PayloadSchemaParams::Datetime(DatetimeIndexParams {
            memory: None,
            r#type: DatetimeIndexType::Datetime,
            is_principal,
            on_disk,
            enable_hnsw,
        })
    }

    fn uuid(
        on_disk: Option<bool>,
        is_tenant: Option<bool>,
        enable_hnsw: Option<bool>,
    ) -> PayloadSchemaParams {
        PayloadSchemaParams::Uuid(UuidIndexParams {
            memory: None,
            r#type: UuidIndexType::Uuid,
            is_tenant,
            on_disk,
            enable_hnsw,
        })
    }

    fn on_disk_only(new_on_disk: bool) -> SchemaTransition {
        SchemaTransition::Compatible(CompatibleDiff {
            on_disk: Some(new_on_disk),
            metadata: false,
        })
    }

    fn metadata_only() -> SchemaTransition {
        SchemaTransition::Compatible(CompatibleDiff {
            on_disk: None,
            metadata: true,
        })
    }

    #[test]
    fn identical_returns_identical() {
        let s = wrap(keyword(Some(false), None, None));
        assert_eq!(classify(&s, &s.clone()), SchemaTransition::Identical);
    }

    #[test]
    fn fieldtype_vs_fieldparams_identical() {
        // FieldType expands to default params; FieldParams with default values must compare equal.
        let by_type = PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword);
        let by_params = wrap(PayloadSchemaParams::Keyword(KeywordIndexParams::default()));
        assert_eq!(classify(&by_type, &by_params), SchemaTransition::Identical);
        assert_eq!(classify(&by_params, &by_type), SchemaTransition::Identical);
    }

    #[test]
    fn keyword_on_disk_flip_only() {
        let off = wrap(keyword(Some(false), None, None));
        let on = wrap(keyword(Some(true), None, None));
        assert_eq!(classify(&off, &on), on_disk_only(true));
        assert_eq!(classify(&on, &off), on_disk_only(false));
    }

    #[test]
    fn keyword_enable_hnsw_flip_only() {
        let enabled = wrap(keyword(Some(false), None, Some(true)));
        let disabled = wrap(keyword(Some(false), None, Some(false)));
        assert_eq!(classify(&enabled, &disabled), metadata_only());
        assert_eq!(classify(&disabled, &enabled), metadata_only());
    }

    #[test]
    fn keyword_on_disk_and_enable_hnsw_flip() {
        let a = wrap(keyword(Some(false), None, Some(true)));
        let b = wrap(keyword(Some(true), None, Some(false)));
        assert_eq!(
            classify(&a, &b),
            SchemaTransition::Compatible(CompatibleDiff {
                on_disk: Some(true),
                metadata: true,
            }),
        );
    }

    #[test]
    fn keyword_prefix_change_is_incompatible() {
        // Enabling or disabling prefix matching requires building or dropping
        // the sorted key dictionary — a full rebuild, never an in-place swap.
        let plain = wrap(keyword(Some(false), None, None));
        let with_prefix = wrap(PayloadSchemaParams::Keyword(KeywordIndexParams {
            memory: None,
            r#type: KeywordIndexType::Keyword,
            is_tenant: None,
            on_disk: Some(false),
            enable_hnsw: None,
            prefix: Some(true),
        }));
        assert_eq!(
            classify(&plain, &with_prefix),
            SchemaTransition::Incompatible
        );
        assert_eq!(
            classify(&with_prefix, &plain),
            SchemaTransition::Incompatible
        );
    }

    #[test]
    fn keyword_other_field_differs_is_incompatible() {
        // Same on_disk, but is_tenant differs.
        let a = wrap(keyword(Some(false), Some(false), None));
        let b = wrap(keyword(Some(false), Some(true), None));
        assert_eq!(classify(&a, &b), SchemaTransition::Incompatible);
        // Both on_disk AND another field differ — also Incompatible (swap
        // can't paper over the other change).
        let c = wrap(keyword(Some(true), Some(true), None));
        assert_eq!(classify(&a, &c), SchemaTransition::Incompatible);
    }

    #[test]
    fn integer_on_disk_flip_only() {
        let off = wrap(integer(Some(false), Some(true), None));
        let on = wrap(integer(Some(true), Some(true), None));
        assert_eq!(classify(&off, &on), on_disk_only(true));
    }

    #[test]
    fn integer_enable_hnsw_flip_only() {
        let a = wrap(integer(Some(false), Some(true), Some(true)));
        let b = wrap(integer(Some(false), Some(true), Some(false)));
        assert_eq!(classify(&a, &b), metadata_only());
    }

    #[test]
    fn integer_lookup_change_is_incompatible() {
        let a = wrap(integer(Some(false), Some(true), None));
        let b = wrap(integer(Some(false), Some(false), None));
        assert_eq!(classify(&a, &b), SchemaTransition::Incompatible);
    }

    #[test]
    fn float_on_disk_flip_only() {
        assert_eq!(
            classify(
                &wrap(float(Some(false), None)),
                &wrap(float(Some(true), None))
            ),
            on_disk_only(true),
        );
    }

    #[test]
    fn geo_on_disk_flip_only() {
        assert_eq!(
            classify(&wrap(geo(Some(false), None)), &wrap(geo(Some(true), None))),
            on_disk_only(true),
        );
    }

    #[test]
    fn text_on_disk_flip_only() {
        assert_eq!(
            classify(
                &wrap(text(Some(false), TokenizerType::Word, None)),
                &wrap(text(Some(true), TokenizerType::Word, None)),
            ),
            on_disk_only(true),
        );
    }

    #[test]
    fn text_tokenizer_change_is_incompatible() {
        assert_eq!(
            classify(
                &wrap(text(Some(false), TokenizerType::Word, None)),
                &wrap(text(Some(false), TokenizerType::Whitespace, None)),
            ),
            SchemaTransition::Incompatible,
        );
    }

    #[test]
    fn bool_on_disk_flip_only() {
        assert_eq!(
            classify(
                &wrap(bool_p(Some(false), None)),
                &wrap(bool_p(Some(true), None))
            ),
            on_disk_only(true),
        );
    }

    #[test]
    fn datetime_on_disk_flip_only() {
        assert_eq!(
            classify(
                &wrap(datetime(Some(false), None, None)),
                &wrap(datetime(Some(true), None, None))
            ),
            on_disk_only(true),
        );
    }

    #[test]
    fn uuid_on_disk_flip_only() {
        assert_eq!(
            classify(
                &wrap(uuid(Some(false), None, None)),
                &wrap(uuid(Some(true), None, None))
            ),
            on_disk_only(true),
        );
    }

    #[test]
    fn cross_kind_is_incompatible() {
        assert_eq!(
            classify(
                &wrap(keyword(Some(false), None, None)),
                &wrap(integer(Some(false), Some(true), None))
            ),
            SchemaTransition::Incompatible,
        );
        assert_eq!(
            classify(
                &wrap(geo(Some(false), None)),
                &wrap(float(Some(false), None))
            ),
            SchemaTransition::Incompatible,
        );
    }

    #[test]
    fn on_disk_none_treated_as_default_false() {
        // Both `None` => Identical (both expand to default).
        let none_a = wrap(keyword(None, None, None));
        let none_b = wrap(keyword(None, None, None));
        assert_eq!(classify(&none_a, &none_b), SchemaTransition::Identical);

        // None vs Some(true) is a flip from default-false to explicit-true.
        let none = wrap(keyword(None, None, None));
        let on = wrap(keyword(Some(true), None, None));
        assert_eq!(classify(&none, &on), on_disk_only(true));

        // None vs Some(false) — both mean "false", so they are Identical.
        // The field-level comparison (`Option<bool>`) sees them as different,
        // but `is_on_disk()` reads `unwrap_or_default()` so they're semantically equal.
        //
        // We choose to surface this as `Compatible { on_disk: Some(false) }` rather than
        // Identical, because the persisted on_disk value differs (None vs Some(false)) and the
        // caller may want the persisted value updated. The swap itself is a no-op in that case.
        let none = wrap(keyword(None, None, None));
        let off = wrap(keyword(Some(false), None, None));
        assert_eq!(classify(&none, &off), on_disk_only(false));
    }

    #[test]
    fn enable_hnsw_none_vs_some_true_is_metadata() {
        // Default enable_hnsw is true; None vs Some(true) still updates the
        // persisted value via the metadata-only path.
        let none = wrap(keyword(Some(false), None, None));
        let explicit = wrap(keyword(Some(false), None, Some(true)));
        assert_eq!(classify(&none, &explicit), metadata_only());
    }

    #[test]
    fn compatible_diff_metadata_only_helper() {
        assert!(
            CompatibleDiff {
                on_disk: None,
                metadata: true,
            }
            .metadata_only()
        );
        assert!(
            !CompatibleDiff {
                on_disk: Some(true),
                metadata: true,
            }
            .metadata_only()
        );
        assert!(
            !CompatibleDiff {
                on_disk: Some(false),
                metadata: false,
            }
            .metadata_only()
        );
    }
}
