//! Classify a payload index schema transition between the previously stored
//! schema and the newly requested one.
//!
//! Used by `StructPayloadIndex::set_indexed` to detect the case where the
//! only difference is the `on_disk` flag. For non-appendable segments this
//! lets us swap the in-memory wrapper variant in place instead of dropping
//! and rebuilding the entire field index from payload storage.
//!
//! Each per-kind arm normalizes `on_disk` on a clone and compares the rest
//! via the derived `PartialEq`, so a newly added field is accounted for
//! automatically: any difference outside `on_disk` yields `Incompatible`.

use crate::types::{PayloadFieldSchema, PayloadSchemaParams};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaTransition {
    /// The two schemas are functionally identical (modulo `FieldType` vs
    /// fully-expanded `FieldParams`).
    Identical,
    /// The two schemas differ only in their `on_disk` flag. Eligible for the
    /// in-place swap fast path on non-appendable segments.
    OnlyOnDiskFlipped { new_on_disk: bool },
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

    if only_on_disk_differs(old, new) {
        return SchemaTransition::OnlyOnDiskFlipped {
            new_on_disk: new.is_on_disk(),
        };
    }

    SchemaTransition::Incompatible
}

fn only_on_disk_differs(old: &PayloadSchemaParams, new: &PayloadSchemaParams) -> bool {
    use PayloadSchemaParams as P;

    // Compare `on_disk` at the `Option<bool>` level: `None` vs `Some(false)`
    // both mean "false" yet the persisted value differs, so it still counts as
    // a flip. Every other field goes through the derived `PartialEq` after
    // normalizing `on_disk` on a clone — a newly added field is therefore
    // accounted for automatically (any difference makes this `false`, i.e.
    // `Incompatible`, the safe default).
    macro_rules! only_on_disk {
        ($a:expr, $b:expr) => {
            $a.on_disk != $b.on_disk && {
                let mut normalized = $a.clone();
                normalized.on_disk = $b.on_disk;
                normalized == *$b
            }
        };
    }

    match (old, new) {
        (P::Keyword(a), P::Keyword(b)) => only_on_disk!(a, b),
        (P::Integer(a), P::Integer(b)) => only_on_disk!(a, b),
        (P::Float(a), P::Float(b)) => only_on_disk!(a, b),
        (P::Geo(a), P::Geo(b)) => only_on_disk!(a, b),
        (P::Text(a), P::Text(b)) => only_on_disk!(a, b),
        (P::Bool(a), P::Bool(b)) => only_on_disk!(a, b),
        (P::Datetime(a), P::Datetime(b)) => only_on_disk!(a, b),
        (P::Uuid(a), P::Uuid(b)) => only_on_disk!(a, b),
        // Cross-kind pairs cannot be "only on_disk differs". Listed
        // exhaustively (rather than `_ =>`) so a new `PayloadSchemaParams`
        // variant triggers a compile error here.
        (P::Keyword(_), _)
        | (P::Integer(_), _)
        | (P::Float(_), _)
        | (P::Geo(_), _)
        | (P::Text(_), _)
        | (P::Bool(_), _)
        | (P::Datetime(_), _)
        | (P::Uuid(_), _) => false,
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

    fn keyword(on_disk: Option<bool>, is_tenant: Option<bool>) -> PayloadSchemaParams {
        PayloadSchemaParams::Keyword(KeywordIndexParams {
            r#type: KeywordIndexType::Keyword,
            is_tenant,
            on_disk,
            enable_hnsw: None,
        })
    }

    fn integer(on_disk: Option<bool>, lookup: Option<bool>) -> PayloadSchemaParams {
        PayloadSchemaParams::Integer(IntegerIndexParams {
            r#type: IntegerIndexType::Integer,
            lookup,
            range: Some(true),
            is_principal: None,
            on_disk,
            enable_hnsw: None,
        })
    }

    fn float(on_disk: Option<bool>) -> PayloadSchemaParams {
        PayloadSchemaParams::Float(FloatIndexParams {
            r#type: FloatIndexType::Float,
            is_principal: None,
            on_disk,
            enable_hnsw: None,
        })
    }

    fn geo(on_disk: Option<bool>) -> PayloadSchemaParams {
        PayloadSchemaParams::Geo(GeoIndexParams {
            r#type: GeoIndexType::Geo,
            on_disk,
            enable_hnsw: None,
        })
    }

    fn text(on_disk: Option<bool>, tokenizer: TokenizerType) -> PayloadSchemaParams {
        PayloadSchemaParams::Text(TextIndexParams {
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
            enable_hnsw: None,
        })
    }

    fn bool_p(on_disk: Option<bool>) -> PayloadSchemaParams {
        PayloadSchemaParams::Bool(BoolIndexParams {
            r#type: BoolIndexType::Bool,
            on_disk,
            enable_hnsw: None,
        })
    }

    fn datetime(on_disk: Option<bool>, is_principal: Option<bool>) -> PayloadSchemaParams {
        PayloadSchemaParams::Datetime(DatetimeIndexParams {
            r#type: DatetimeIndexType::Datetime,
            is_principal,
            on_disk,
            enable_hnsw: None,
        })
    }

    fn uuid(on_disk: Option<bool>, is_tenant: Option<bool>) -> PayloadSchemaParams {
        PayloadSchemaParams::Uuid(UuidIndexParams {
            r#type: UuidIndexType::Uuid,
            is_tenant,
            on_disk,
            enable_hnsw: None,
        })
    }

    #[test]
    fn identical_returns_identical() {
        let s = wrap(keyword(Some(false), None));
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
        let off = wrap(keyword(Some(false), None));
        let on = wrap(keyword(Some(true), None));
        assert_eq!(
            classify(&off, &on),
            SchemaTransition::OnlyOnDiskFlipped { new_on_disk: true },
        );
        assert_eq!(
            classify(&on, &off),
            SchemaTransition::OnlyOnDiskFlipped { new_on_disk: false },
        );
    }

    #[test]
    fn keyword_other_field_differs_is_incompatible() {
        // Same on_disk, but is_tenant differs.
        let a = wrap(keyword(Some(false), Some(false)));
        let b = wrap(keyword(Some(false), Some(true)));
        assert_eq!(classify(&a, &b), SchemaTransition::Incompatible);
        // Both on_disk AND another field differ — also Incompatible (swap
        // can't paper over the other change).
        let c = wrap(keyword(Some(true), Some(true)));
        assert_eq!(classify(&a, &c), SchemaTransition::Incompatible);
    }

    #[test]
    fn integer_on_disk_flip_only() {
        let off = wrap(integer(Some(false), Some(true)));
        let on = wrap(integer(Some(true), Some(true)));
        assert_eq!(
            classify(&off, &on),
            SchemaTransition::OnlyOnDiskFlipped { new_on_disk: true },
        );
    }

    #[test]
    fn integer_lookup_change_is_incompatible() {
        let a = wrap(integer(Some(false), Some(true)));
        let b = wrap(integer(Some(false), Some(false)));
        assert_eq!(classify(&a, &b), SchemaTransition::Incompatible);
    }

    #[test]
    fn float_on_disk_flip_only() {
        assert_eq!(
            classify(&wrap(float(Some(false))), &wrap(float(Some(true)))),
            SchemaTransition::OnlyOnDiskFlipped { new_on_disk: true },
        );
    }

    #[test]
    fn geo_on_disk_flip_only() {
        assert_eq!(
            classify(&wrap(geo(Some(false))), &wrap(geo(Some(true)))),
            SchemaTransition::OnlyOnDiskFlipped { new_on_disk: true },
        );
    }

    #[test]
    fn text_on_disk_flip_only() {
        assert_eq!(
            classify(
                &wrap(text(Some(false), TokenizerType::Word)),
                &wrap(text(Some(true), TokenizerType::Word)),
            ),
            SchemaTransition::OnlyOnDiskFlipped { new_on_disk: true },
        );
    }

    #[test]
    fn text_tokenizer_change_is_incompatible() {
        assert_eq!(
            classify(
                &wrap(text(Some(false), TokenizerType::Word)),
                &wrap(text(Some(false), TokenizerType::Whitespace)),
            ),
            SchemaTransition::Incompatible,
        );
    }

    #[test]
    fn bool_on_disk_flip_only() {
        assert_eq!(
            classify(&wrap(bool_p(Some(false))), &wrap(bool_p(Some(true)))),
            SchemaTransition::OnlyOnDiskFlipped { new_on_disk: true },
        );
    }

    #[test]
    fn datetime_on_disk_flip_only() {
        assert_eq!(
            classify(
                &wrap(datetime(Some(false), None)),
                &wrap(datetime(Some(true), None))
            ),
            SchemaTransition::OnlyOnDiskFlipped { new_on_disk: true },
        );
    }

    #[test]
    fn uuid_on_disk_flip_only() {
        assert_eq!(
            classify(
                &wrap(uuid(Some(false), None)),
                &wrap(uuid(Some(true), None))
            ),
            SchemaTransition::OnlyOnDiskFlipped { new_on_disk: true },
        );
    }

    #[test]
    fn cross_kind_is_incompatible() {
        assert_eq!(
            classify(
                &wrap(keyword(Some(false), None)),
                &wrap(integer(Some(false), Some(true)))
            ),
            SchemaTransition::Incompatible,
        );
        assert_eq!(
            classify(&wrap(geo(Some(false))), &wrap(float(Some(false)))),
            SchemaTransition::Incompatible,
        );
    }

    #[test]
    fn on_disk_none_treated_as_default_false() {
        // Both `None` => Identical (both expand to default).
        let none_a = wrap(keyword(None, None));
        let none_b = wrap(keyword(None, None));
        assert_eq!(classify(&none_a, &none_b), SchemaTransition::Identical);

        // None vs Some(true) is a flip from default-false to explicit-true.
        let none = wrap(keyword(None, None));
        let on = wrap(keyword(Some(true), None));
        assert_eq!(
            classify(&none, &on),
            SchemaTransition::OnlyOnDiskFlipped { new_on_disk: true },
        );

        // None vs Some(false) — both mean "false", so they are Identical.
        // The field-level comparison (`Option<bool>`) sees them as different,
        // but `is_on_disk()` reads `unwrap_or_default()` so they're semantically equal.
        //
        // We choose to surface this as `OnlyOnDiskFlipped { new_on_disk: false }` rather than
        // Identical, because the persisted on_disk value differs (None vs Some(false)) and the
        // caller may want the persisted value updated. The swap itself is a no-op in that case.
        let none = wrap(keyword(None, None));
        let off = wrap(keyword(Some(false), None));
        assert_eq!(
            classify(&none, &off),
            SchemaTransition::OnlyOnDiskFlipped { new_on_disk: false },
        );
    }
}
