//! Type-level boolean and option types.
//!
//! You can use a value of these traits to conditionally enable or disable
//! particular functions in generic contexts.
//! When the value is [`False`], the code is still successfully type-checked,
//! but never executed.
//!
//! ```rust
//! use common::tbool::{TBool, True, False};
//!
//! fn foo<E: TBool>(enabled: E) -> i32 {
//!     42
//! }
//!
//! fn bar<E: TBool>(enabled: E) -> i32 {
//!     foo(enabled)
//! }
//!
//! fn baz<E: TBool>() -> String {
//!    match E::VALUE {
//!        Some(e) => format!("enabled: {}", foo(e)),
//!        None => format!("disabled"),
//!    }
//! }
//!
//! fn main() {
//!    assert_eq!(baz::<True>(), "enabled: 42");
//!    assert_eq!(baz::<False>(), "disabled");
//! }
//! ```
//!
//! Compared to using [`bool`] or [`Option`], there are no runtime checks, nor
//! performance overhead, nor potential panics due to unwrapping an [`Option`].

use std::marker::PhantomData;

use bytemuck::TransparentWrapper;

/// A boolean value lifted to the type level.
///
/// Either [`True`] or [`False`].
pub trait TBool: Sized + Clone + Copy {
    type When<T>: When<T, TBool = Self>;

    /// Either `Some(True)` or `None`.
    const VALUE: Option<Self>;

    /// Wrap a value of type `T`.
    ///
    /// Similar to [`bool::then_some`] but lifted to the type level.
    ///
    /// Returns either:
    /// - `Some(WhenTrue(value))` if `Self` is [`True`],
    /// - `None` if `Self` is [`False`].
    fn when<T>(value: T) -> Option<Self::When<T>>;

    /// Similar to [`TBool::when`], but for references.
    fn when_ref<T>(value: &T) -> Option<&Self::When<T>>;
}

/// An [`Option`]-like wrapper but lifted to the type level.
///
/// Tied to the corresponding [`TBool`] type.
/// Either [`WhenTrue`] or [`WhenFalse`].
pub trait When<T> {
    type TBool: TBool;

    /// Get the wrapped value.
    ///
    /// Similar to [`Option::unwrap`], but always succeeds and does not panic.
    fn get(&self) -> &T;

    /// Get the corresponding [`TBool`] value.
    ///
    /// Similar to [`Option::is_some`].
    fn enabled(&self) -> Self::TBool;
}

/// A [`TBool`] implementation of `true`.
///
/// This type is a zero-sized unit type.
#[derive(Debug, Clone, Copy)]
pub struct True;

/// A [`TBool`] implementation of `false`.
///
/// This type can not be instantiated (similar to the [never] type).
///
/// [never]: https://doc.rust-lang.org/std/primitive.never.html
#[derive(Debug, Clone, Copy)]
pub enum False {}

/// A [`When`] type that corresponds to [`True`].
///
/// Similar to [`Option::Some`] but lifted to the type level.
/// This type is a transparent wrapper around a value of type `T`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, TransparentWrapper)]
#[repr(transparent)]
pub struct WhenTrue<T>(pub T);

/// A [`When`] type that corresponds to [`False`].
///
/// Similar to [`Option::None`] but lifted to the type level.
/// This type can not be instantiated (similar to the [never] type).
///
/// [never]: https://doc.rust-lang.org/std/primitive.never.html
#[derive(Debug, Clone, Copy)]
pub struct WhenFalse<T>(False, PhantomData<T>);

impl TBool for True {
    type When<T> = WhenTrue<T>;

    const VALUE: Option<Self> = Some(True);

    #[inline]
    fn when<T>(value: T) -> Option<Self::When<T>> {
        Some(WhenTrue(value))
    }

    #[inline]
    fn when_ref<T>(value: &T) -> Option<&Self::When<T>> {
        Some(WhenTrue::wrap_ref(value))
    }
}

impl<T> When<T> for WhenTrue<T> {
    type TBool = True;

    #[inline]
    fn get(&self) -> &T {
        &self.0
    }

    #[inline]
    fn enabled(&self) -> Self::TBool {
        True
    }
}

impl TBool for False {
    type When<T> = WhenFalse<T>;

    const VALUE: Option<Self> = None;

    #[inline]
    fn when<T>(_: T) -> Option<Self::When<T>> {
        None
    }

    #[inline]
    fn when_ref<T>(_: &T) -> Option<&Self::When<T>> {
        None
    }
}

impl<T> When<T> for WhenFalse<T> {
    type TBool = False;

    fn get(&self) -> &T {
        match self.0 {}
    }

    fn enabled(&self) -> Self::TBool {
        match self.0 {}
    }
}
