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
