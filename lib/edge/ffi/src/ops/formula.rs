//! [`Expression`] — the formula AST used by
//! [`ScoringQuery::Formula`](crate::ops::query::ScoringQuery).
//!
//! An expression tree is recursive, and UniFFI records cannot recurse through
//! `Box`, so — like [`UpdateOperation`](crate::update::UpdateOperation) — the
//! tree node is an opaque object built via validating constructors and
//! composed via handles. Each constructor validates its direct inputs and
//! tracks two quantities that together keep a hostile host from crashing the
//! process: nesting **depth** (so the recursive clone/convert/drop cannot
//! overflow the stack) and total **node count** (so the eager
//! `ExpressionInternal` clone cannot exhaust the heap — a host can reuse one
//! handle as several children, growing node count as `2^depth` while depth
//! stays small). See [`crate::error::check_expression_size`].

use std::sync::Arc;

use segment::index::query_optimization::rescore_formula::parsed_formula::DecayKind as SegmentDecayKind;
use segment::types::{Condition as SegmentCondition, GeoPoint as SegmentGeoPoint};
use shard::query::formula::ExpressionInternal;

use crate::error::{EdgeError, Result, check_expression_size, check_nesting_depth};
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
    /// Total node count of this subtree (saturating); constructors reject trees
    /// larger than [`MAX_FORMULA_NODES`](crate::error::MAX_FORMULA_NODES) so the
    /// eager `inner` clone cannot exhaust the heap. Depth alone does not bound
    /// this — a reused handle grows node count as `2^depth`.
    size: u64,
}

impl Expression {
    fn leaf(inner: ExpressionInternal) -> Arc<Self> {
        Arc::new(Self {
            inner,
            depth: 0,
            size: 1,
        })
    }

    /// Wrap a node one level above `children`, enforcing both the depth cap
    /// (stack safety) and the node-count cap (heap safety) **before** `inner` is
    /// materialized.
    ///
    /// `build` is invoked only once both checks pass, so a rejected tree never
    /// performs the eager `inner` deep-clone. This ordering is load-bearing: a
    /// host can pass one accepted handle as many `children` (e.g.
    /// `sum(vec![e; N])`), and cloning first would allocate `N × e.size` nodes —
    /// the multi-gigabyte, uncatchable-abort blow-up — before the size check
    /// could reject it. Size is computed from the cheap child handles, not the
    /// cloned tree.
    fn node(
        children: &[&Arc<Expression>],
        build: impl FnOnce() -> ExpressionInternal,
    ) -> Result<Arc<Self>> {
        let depth = children
            .iter()
            .map(|c| c.depth)
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        check_nesting_depth("formula expression", depth)?;
        let size = children
            .iter()
            .fold(1u64, |acc, c| acc.saturating_add(c.size));
        check_expression_size(size)?;
        Ok(Arc::new(Self {
            inner: build(),
            depth,
            size,
        }))
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
        let children: Vec<&Arc<Expression>> = factors.iter().collect();
        Self::node(&children, || {
            ExpressionInternal::Mult(factors.iter().map(|e| e.inner.clone()).collect())
        })
    }

    /// Sum of `terms`.
    #[uniffi::constructor]
    pub fn sum(terms: Vec<Arc<Expression>>) -> Result<Arc<Self>> {
        let children: Vec<&Arc<Expression>> = terms.iter().collect();
        Self::node(&children, || {
            ExpressionInternal::Sum(terms.iter().map(|e| e.inner.clone()).collect())
        })
    }

    /// Negation: `-expression`.
    #[uniffi::constructor]
    pub fn negate(expression: Arc<Expression>) -> Result<Arc<Self>> {
        Self::node(&[&expression], || {
            ExpressionInternal::Neg(Box::new(expression.inner.clone()))
        })
    }

    /// Division: `left / right`. If `right` evaluates to zero, the result is
    /// `by_zero_default` when set; when unset, the division yields a non-finite
    /// value that fails the whole query with an error (it is *not* silently
    /// skipped). A zero numerator always short-circuits to `0.0`.
    #[uniffi::constructor(default(by_zero_default = None))]
    pub fn div(
        left: Arc<Expression>,
        right: Arc<Expression>,
        by_zero_default: Option<f32>,
    ) -> Result<Arc<Self>> {
        // A non-finite fallback would reach the scorer unchecked; reject it at
        // construction, mirroring `constant`.
        if by_zero_default.is_some_and(|d| !d.is_finite()) {
            return Err(EdgeError::invalid_argument(
                "div by_zero_default must be finite",
            ));
        }
        Self::node(&[&left, &right], || ExpressionInternal::Div {
            left: Box::new(left.inner.clone()),
            right: Box::new(right.inner.clone()),
            by_zero_default,
        })
    }

    /// Square root.
    #[uniffi::constructor]
    pub fn sqrt(expression: Arc<Expression>) -> Result<Arc<Self>> {
        Self::node(&[&expression], || {
            ExpressionInternal::Sqrt(Box::new(expression.inner.clone()))
        })
    }

    /// `base` raised to `exponent`.
    #[uniffi::constructor]
    pub fn pow(base: Arc<Expression>, exponent: Arc<Expression>) -> Result<Arc<Self>> {
        Self::node(&[&base, &exponent], || ExpressionInternal::Pow {
            base: Box::new(base.inner.clone()),
            exponent: Box::new(exponent.inner.clone()),
        })
    }

    /// Natural exponent: `e^expression`.
    #[uniffi::constructor]
    pub fn exp(expression: Arc<Expression>) -> Result<Arc<Self>> {
        Self::node(&[&expression], || {
            ExpressionInternal::Exp(Box::new(expression.inner.clone()))
        })
    }

    /// Base-10 logarithm.
    #[uniffi::constructor]
    pub fn log10(expression: Arc<Expression>) -> Result<Arc<Self>> {
        Self::node(&[&expression], || {
            ExpressionInternal::Log10(Box::new(expression.inner.clone()))
        })
    }

    /// Natural logarithm.
    #[uniffi::constructor]
    pub fn ln(expression: Arc<Expression>) -> Result<Arc<Self>> {
        Self::node(&[&expression], || {
            ExpressionInternal::Ln(Box::new(expression.inner.clone()))
        })
    }

    /// Absolute value.
    #[uniffi::constructor]
    pub fn abs(expression: Arc<Expression>) -> Result<Arc<Self>> {
        Self::node(&[&expression], || {
            ExpressionInternal::Abs(Box::new(expression.inner.clone()))
        })
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
        let size = 1u64
            .saturating_add(x.size)
            .saturating_add(target.as_ref().map_or(0, |t| t.size));
        check_expression_size(size)?;
        // Non-finite `midpoint`/`scale` would slip past the engine's
        // comparison-based range checks (every comparison is `false` for NaN),
        // yielding a NaN decay lambda: a debug-build panic on the search worker
        // (crossing the FFI boundary) or a silent all-zero rescore in release.
        // Reject at construction, mirroring `constant`.
        if midpoint.is_some_and(|m| !m.is_finite()) {
            return Err(EdgeError::invalid_argument("decay midpoint must be finite"));
        }
        if scale.is_some_and(|s| !s.is_finite()) {
            return Err(EdgeError::invalid_argument("decay scale must be finite"));
        }
        Ok(Arc::new(Self {
            inner: ExpressionInternal::Decay {
                kind: SegmentDecayKind::from(kind),
                x: Box::new(x.inner.clone()),
                target: target.map(|t| Box::new(t.inner.clone())),
                midpoint,
                scale,
            },
            depth,
            size,
        }))
    }
}

// ── Coverage map ────────────────────────────────────────────────────────────

/// Compile-time map of the engine's formula-expression tree onto the
/// [`Expression`] constructors above — same contract as the maps in
/// [`crate::update`], [`crate::ops::query`], and [`crate::filter`]: no
/// wildcard arms, so a variant added to [`ExpressionInternal`] or the
/// engine's [`DecayKind`](SegmentDecayKind) stops this function from
/// compiling until the constructor surface decision is recorded here.
///
/// Never called; it exists only for the exhaustiveness check.
#[allow(dead_code)]
fn assert_every_expression_is_mapped(e: ExpressionInternal) {
    match e {
        // [`Expression::constant`]
        ExpressionInternal::Constant(_) => {}
        // [`Expression::variable`]
        ExpressionInternal::Variable(_) => {}
        // [`Expression::condition`]
        ExpressionInternal::Condition(_) => {}
        // [`Expression::geo_distance`]
        ExpressionInternal::GeoDistance { .. } => {}
        // [`Expression::datetime`]
        ExpressionInternal::Datetime(_) => {}
        // [`Expression::datetime_key`]
        ExpressionInternal::DatetimeKey(_) => {}
        // [`Expression::mult`]
        ExpressionInternal::Mult(_) => {}
        // [`Expression::sum`]
        ExpressionInternal::Sum(_) => {}
        // [`Expression::negate`]
        ExpressionInternal::Neg(_) => {}
        // [`Expression::div`]
        ExpressionInternal::Div { .. } => {}
        // [`Expression::sqrt`]
        ExpressionInternal::Sqrt(_) => {}
        // [`Expression::pow`]
        ExpressionInternal::Pow { .. } => {}
        // [`Expression::exp`]
        ExpressionInternal::Exp(_) => {}
        // [`Expression::log10`]
        ExpressionInternal::Log10(_) => {}
        // [`Expression::ln`]
        ExpressionInternal::Ln(_) => {}
        // [`Expression::abs`]
        ExpressionInternal::Abs(_) => {}
        // [`Expression::decay`], with every [`DecayKind`]
        ExpressionInternal::Decay { kind, .. } => match kind {
            // [`DecayKind::Lin`]
            SegmentDecayKind::Lin => {}
            // [`DecayKind::Gauss`]
            SegmentDecayKind::Gauss => {}
            // [`DecayKind::Exp`]
            SegmentDecayKind::Exp => {}
        },
    }
}
