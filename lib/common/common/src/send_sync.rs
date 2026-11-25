//! Compile-time optimization for the auto traits `Send` and `Sync`.

/// Implements `Send` and `Sync` for a struct whose fields are all `Send + Sync`.
///
/// The resulting impls are equivalent to the auto-derived ones, but they stop the trait solver
/// from walking through all the fields every time it has to prove `T: Send` or `T: Sync`.
///
/// This matters for large types like `TableOfContent` or `Collection`, which transitively contain
/// almost every type in the storage stack. `#[async_trait]` (used by the tonic services) adds
/// `Self: 'async_trait` bounds, so each service impl gets its own trait-solver cache, and proving
/// the returned future is `Send` walks the whole type tree again. With these impls, that walk
/// stops at the annotated struct.
///
/// The field list must be exhaustive: it destructures the struct without `..`, so adding a field
/// without listing it here is a compile error, and every listed field must be `Send + Sync`.
///
/// ```
/// struct Foo {
///     a: u32,
///     b: std::sync::Arc<String>,
/// }
/// common::impl_send_sync_checked!(Foo { a, b });
/// ```
///
/// ```compile_fail
/// struct Foo {
///     a: u32,
///     b: std::rc::Rc<String>,
/// }
/// common::impl_send_sync_checked!(Foo { a, b });
/// ```
///
/// ```compile_fail
/// struct Foo {
///     a: u32,
///     b: std::sync::Arc<String>,
/// }
/// common::impl_send_sync_checked!(Foo { a });
/// ```
#[macro_export]
macro_rules! impl_send_sync_checked {
    ($ty:ident { $($field:ident),* $(,)? }) => {
        // SAFETY: every field of the struct is checked to be `Send + Sync` below.
        // The destructuring is exhaustive, so no field can be left unchecked.
        unsafe impl Send for $ty {}
        // SAFETY: see above.
        unsafe impl Sync for $ty {}

        const _: () = {
            fn assert_send_sync<T: Send + Sync + ?Sized>(_: &T) {}

            #[allow(dead_code)]
            fn assert_fields_send_sync(value: &$ty) {
                let $ty { $($field),* } = value;
                $(assert_send_sync($field);)*
            }
        };
    };
}
