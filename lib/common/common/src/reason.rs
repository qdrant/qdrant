/// A compile-time proof that a reason is provided.
///
/// These "mandatory reasons" are conceptually similar:
///
/// ```ignore
/// #[expect(clippy::…, reason = "Blahblah")]
/// ( code that triggers the lint )
///
/// // SAFETY: Blahblah
/// unsafe { … }
///
/// dangerous_function(reason("Blahblah"), …);
/// ```
#[derive(Clone, Copy, Debug)]
pub struct Reason(());

/// Create a [Reason] token. The text is ignored.
#[inline(always)]
pub const fn reason(_: &'static str) -> Reason {
    Reason(())
}

/// Commonly used reasons.
impl Reason {
    pub const EDGE_UNMEASURED: Reason = reason("Edge doesn't report hardware usage (yet?)");

    #[cfg(any(test, feature = "testing"))]
    pub const TEST: Reason = reason("Test or benchmark code");
}
