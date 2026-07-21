//! [`Expression`] — the formula AST used by
//! [`ScoringQuery::Formula`](crate::ops::query::ScoringQuery).
//!
//! An expression tree is recursive, and UniFFI records cannot recurse through
//! `Box`, so — like [`UpdateOperation`](crate::update::UpdateOperation) — the
//! tree node is an opaque object built via validating constructors and
//! composed via handles. Each constructor validates its direct inputs and
//! tracks nesting depth, so a hostile host cannot build a tree deep enough to
//! overflow the stack in the conversion/drop recursion.

use std::sync::Arc;

use segment::index::query_optimization::rescore_formula::parsed_formula::DecayKind as SegmentDecayKind;
use segment::types::{Condition as SegmentCondition, GeoPoint as SegmentGeoPoint};
use shard::query::formula::ExpressionInternal;

use crate::error::{EdgeError, Result, check_nesting_depth};
use crate::filter::{Condition, GeoPoint};

// ── DecayKind ───────────────────────────────────────────────────────────────

/// The shape of a decay function used by [`Expression::decay`].
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum DecayKind {
    /// Linear decay: score falls off linearly and reaches zero at
    /// `scale` distance from the target.
    Lin,
    /// Gaussian decay: smooth bell-curve fall-off.
    Gauss,
    /// Exponential decay: sharp fall-off near the target, long tail.
    Exp,
}

impl From<DecayKind> for SegmentDecayKind {
    fn from(k: DecayKind) -> Self {
        match k {
            DecayKind::Lin => SegmentDecayKind::Lin,
            DecayKind::Gauss => SegmentDecayKind::Gauss,
            DecayKind::Exp => SegmentDecayKind::Exp,
        }
    }
}

// ── Expression ──────────────────────────────────────────────────────────────

/// A node of a score-boosting formula, used by
/// [`ScoringQuery::Formula`](crate::ops::query::ScoringQuery).
///
/// Build a tree bottom-up via the static constructors and pass the root to
/// the query. Variables reference the prefetch scores (`"$score"`,
/// `"$score[1]"`, …) or payload fields by JSON path.
///
/// ## Example
///
/// ```swift
/// // score + 0.2 * popularity
/// let formula = try Expression.sum(terms: [
///     try Expression.variable(name: "$score"),
///     try Expression.mult(factors: [
///         Expression.constant(value: 0.2),
///         try Expression.variable(name: "popularity"),
///     ]),
/// ])
/// ```
///
/// ```kotlin
/// val formula = Expression.sum(listOf(
///     Expression.variable("\$score"),
///     Expression.mult(listOf(
///         Expression.constant(0.2f),
///         Expression.variable("popularity"),
///     )),
/// ))
/// ```
#[derive(Debug, uniffi::Object)]
pub struct Expression {
    pub(crate) inner: ExpressionInternal,
    /// Height of this node's subtree; constructors reject trees deeper than
    /// [`MAX_QUERY_NESTING_DEPTH`](crate::error::MAX_QUERY_NESTING_DEPTH) so
    /// the recursive clone/convert/drop over `inner` stays stack-safe.
    depth: u32,
}

impl Expression {
    fn leaf(inner: ExpressionInternal) -> Arc<Self> {
        Arc::new(Self { inner, depth: 0 })
    }

    /// Wrap `inner` one level above `children`, enforcing the depth cap.
    fn node(inner: ExpressionInternal, children: &[&Arc<Expression>]) -> Result<Arc<Self>> {
        let depth = children
            .iter()
            .map(|c| c.depth)
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        check_nesting_depth("formula expression", depth)?;
        Ok(Arc::new(Self { inner, depth }))
    }
}

#[uniffi::export]
impl Expression {
    /// A constant number.
    #[uniffi::constructor]
    pub fn constant(value: f32) -> Result<Arc<Self>> {
        if !value.is_finite() {
            return Err(EdgeError::invalid_argument(
                "formula constant must be finite",
            ));
        }
        Ok(Self::leaf(ExpressionInternal::Constant(value)))
    }

    /// A variable: `"$score"` / `"$score[n]"` for the n-th prefetch score,
    /// or a payload key in JSON-path syntax.
    ///
    /// The name is validated when the query is executed; an unknown variable
    /// form is rejected there as `InvalidArgument`.
    #[uniffi::constructor]
    pub fn variable(name: String) -> Arc<Self> {
        Self::leaf(ExpressionInternal::Variable(name))
    }

    /// A filter condition evaluated per point: `1.0` when it matches, `0.0`
    /// otherwise.
    #[uniffi::constructor]
    pub fn condition(condition: Condition) -> Result<Arc<Self>> {
        let condition = SegmentCondition::try_from(condition)?;
        Ok(Self::leaf(ExpressionInternal::Condition(Box::new(
            condition,
        ))))
    }

    /// Haversine distance (meters) between `origin` and the geo point stored
    /// at payload key `to`.
    #[uniffi::constructor]
    pub fn geo_distance(origin: GeoPoint, to: String) -> Result<Arc<Self>> {
        Ok(Self::leaf(ExpressionInternal::GeoDistance {
            origin: SegmentGeoPoint::try_from(origin)?,
            to: crate::error::parse_json_path(&to)?,
        }))
    }

    /// A constant RFC 3339 datetime value, comparable with datetime payload
    /// keys. Validated when the query is executed.
    #[uniffi::constructor]
    pub fn datetime(value: String) -> Arc<Self> {
        Self::leaf(ExpressionInternal::Datetime(value))
    }

    /// References a payload key holding datetime values.
    #[uniffi::constructor]
    pub fn datetime_key(key: String) -> Result<Arc<Self>> {
        Ok(Self::leaf(ExpressionInternal::DatetimeKey(
            crate::error::parse_json_path(&key)?,
        )))
    }

    /// Product of `factors`.
    #[uniffi::constructor]
    pub fn mult(factors: Vec<Arc<Expression>>) -> Result<Arc<Self>> {
        Self::node(
            ExpressionInternal::Mult(factors.iter().map(|e| e.inner.clone()).collect()),
            &factors.iter().collect::<Vec<_>>(),
        )
    }

    /// Sum of `terms`.
    #[uniffi::constructor]
    pub fn sum(terms: Vec<Arc<Expression>>) -> Result<Arc<Self>> {
        Self::node(
            ExpressionInternal::Sum(terms.iter().map(|e| e.inner.clone()).collect()),
            &terms.iter().collect::<Vec<_>>(),
        )
    }

    /// Negation: `-expression`.
    #[uniffi::constructor]
    pub fn negate(expression: Arc<Expression>) -> Result<Arc<Self>> {
        Self::node(
            ExpressionInternal::Neg(Box::new(expression.inner.clone())),
            &[&expression],
        )
    }

    /// Division: `left / right`. When `right` evaluates to zero the result is
    /// `by_zero_default`, or the point is discarded when unset.
    #[uniffi::constructor(default(by_zero_default = None))]
    pub fn div(
        left: Arc<Expression>,
        right: Arc<Expression>,
        by_zero_default: Option<f32>,
    ) -> Result<Arc<Self>> {
        Self::node(
            ExpressionInternal::Div {
                left: Box::new(left.inner.clone()),
                right: Box::new(right.inner.clone()),
                by_zero_default,
            },
            &[&left, &right],
        )
    }

    /// Square root.
    #[uniffi::constructor]
    pub fn sqrt(expression: Arc<Expression>) -> Result<Arc<Self>> {
        Self::node(
            ExpressionInternal::Sqrt(Box::new(expression.inner.clone())),
            &[&expression],
        )
    }

    /// `base` raised to `exponent`.
    #[uniffi::constructor]
    pub fn pow(base: Arc<Expression>, exponent: Arc<Expression>) -> Result<Arc<Self>> {
        Self::node(
            ExpressionInternal::Pow {
                base: Box::new(base.inner.clone()),
                exponent: Box::new(exponent.inner.clone()),
            },
            &[&base, &exponent],
        )
    }

    /// Natural exponent: `e^expression`.
    #[uniffi::constructor]
    pub fn exp(expression: Arc<Expression>) -> Result<Arc<Self>> {
        Self::node(
            ExpressionInternal::Exp(Box::new(expression.inner.clone())),
            &[&expression],
        )
    }

    /// Base-10 logarithm.
    #[uniffi::constructor]
    pub fn log10(expression: Arc<Expression>) -> Result<Arc<Self>> {
        Self::node(
            ExpressionInternal::Log10(Box::new(expression.inner.clone())),
            &[&expression],
        )
    }

    /// Natural logarithm.
    #[uniffi::constructor]
    pub fn ln(expression: Arc<Expression>) -> Result<Arc<Self>> {
        Self::node(
            ExpressionInternal::Ln(Box::new(expression.inner.clone())),
            &[&expression],
        )
    }

    /// Absolute value.
    #[uniffi::constructor]
    pub fn abs(expression: Arc<Expression>) -> Result<Arc<Self>> {
        Self::node(
            ExpressionInternal::Abs(Box::new(expression.inner.clone())),
            &[&expression],
        )
    }

    /// Decay function over `x`: `1.0` at `target` (default `0`), decaying to
    /// `midpoint` (default `0.5`, must be in `(0, 1)`) at `scale` (default
    /// `1.0`, must be positive) distance from it. Parameter ranges are
    /// validated when the query is executed.
    #[uniffi::constructor(default(target = None, midpoint = None, scale = None))]
    pub fn decay(
        kind: DecayKind,
        x: Arc<Expression>,
        target: Option<Arc<Expression>>,
        midpoint: Option<f32>,
        scale: Option<f32>,
    ) -> Result<Arc<Self>> {
        let child_depth = x.depth.max(target.as_ref().map_or(0, |t| t.depth));
        let depth = child_depth.saturating_add(1);
        check_nesting_depth("formula expression", depth)?;
        Ok(Arc::new(Self {
            inner: ExpressionInternal::Decay {
                kind: SegmentDecayKind::from(kind),
                x: Box::new(x.inner.clone()),
                target: target.map(|t| Box::new(t.inner.clone())),
                midpoint,
                scale,
            },
            depth,
        }))
    }
}
