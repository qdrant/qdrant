//! Type-level boolean and option types.
//!
//! A value of these traits can be used to conditionally enable or disable
//! particular functions in generic contexts. Compared to using [`bool`] or
//! [`Option`], there are no runtime checks, nor performance overhead, nor
//! potential panics due to unwrapping an [`Option`].
//!
//! # Example
//!
//! ```
//! use common::typelevel::{TBool, True, False};
//!
//! trait MyTrait {
//!     /// Whether the optional method `increment_value` can be called.
//!     type CanIncrement: TBool;
//!     /// Optional method. Because it accepts `CanIncrement` as an argument,
//!     /// it can only be called if `CanIncrement` is `True`.
//!     fn increment_value(&mut self, proof: Self::CanIncrement) -> u32;
//! }
//!
//! struct Unit;
//! impl MyTrait for Unit {
//!     type CanIncrement = False;
//!     fn increment_value(&mut self, proof: Self::CanIncrement) -> u32 {
//!         // The value of type `False` is a proof to the compiler that this
//!         // method cannot be called. It can be coerced to any type using an
//!         // empty match.
//!         match proof {} // coerced to `u32`
//!     }
//! }
//!
//! struct Counter(u32);
//! impl MyTrait for Counter {
//!     type CanIncrement = True;
//!     fn increment_value(&mut self, _proof: Self::CanIncrement) -> u32 {
//!         self.0 += 1;
//!         self.0
//!     }
//! }
//!
//! /// This function calls `increment_value` twice. It requires a proof that
//! /// `increment_value` can be called, and passes it to the method. Even if
//! /// `CanIncrement` is `False`, this function still type checks, but can
//! /// not be called. Note that there are no runtime checks in this method.
//! fn increment_twice<T: MyTrait>(proof: T::CanIncrement, x: &mut T) -> u32 {
//!     x.increment_value(proof);
//!     x.increment_value(proof)
//! }
//!
//! fn run<T: MyTrait>(x: &mut T) {
//!     // The following `if` is the only runtime check in this example.
//!     if let Some(enabled) = T::CanIncrement::VALUE {
//!         let v = increment_twice(enabled, x);
//!         println!("After incrementing twice: {v}");
//!     }
//! }
//!
//! fn main() {
//!     run(&mut Unit);
//!     run(&mut Counter(100));
//! }
//! ```

use std::marker::PhantomData;

use bytemuck::TransparentWrapper;

/// A boolean value lifted to the type level.
pub trait TBool: Sized + Clone + Copy {
    /// Either `Some(True)` or `None`.
    const VALUE: Option<Self>;

    /// The corresponding [`trait TOption`] type.
    type TOption<T>: TOption<T, IsSome = Self>;

    /// Wrap a value of type `T`. Similar to [`bool::then_some`] but lifted to
    /// the type level.
    ///
    /// Returns either:
    /// - `Some(TSome(value))` if `Self` is [`True`],
    /// - `None` if `Self` is [`False`].
    fn then_some<T>(value: T) -> Option<Self::TOption<T>>;

    /// Similar to [`TBool::then_some`], but for references.
    fn then_some_ref<T>(value: &T) -> Option<&Self::TOption<T>>;
}

/// An [`Option`]-like wrapper lifted to the type level.
pub trait TOption<T> {
    /// The corresponding [`TBool`] type. Similar to [`Option::is_some`].
    type IsSome: TBool;

    /// Get the corresponding [`TBool`] value. Similar to [`Option::is_some`].
    fn is_some(&self) -> Self::IsSome;

    /// Get the wrapped value. Similar to [`Option::unwrap`] but always succeeds
    /// and does not panic.
    fn get(&self) -> &T;
}

/// A [`TBool`] implementation of `true`.
///
/// This type is a zero-sized unit type.
#[derive(Debug, Clone, Copy)]
pub struct True;

/// A [`TBool`] implementation of `false`.
///
/// This type cannot be instantiated, similar to the [never] type.
///
/// [never]: https://doc.rust-lang.org/std/primitive.never.html
#[derive(Debug, Clone, Copy)]
pub enum False {}

/// A [`TOption`] type that corresponds to [`True`].
///
/// Similar to [`Option::Some`] but lifted to the type level.
/// This type is a transparent wrapper around a value of type `T`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, TransparentWrapper)]
#[repr(transparent)]
pub struct TSome<T>(pub T);

/// A [`TOption`] type that corresponds to [`False`].
///
/// Similar to [`Option::None`] but lifted to the type level.
/// This type cannot be instantiated (similar to the [never] type).
///
/// [never]: https://doc.rust-lang.org/std/primitive.never.html
#[derive(Debug, Clone, Copy)]
pub struct TNone<T>(False, PhantomData<T>);

impl TBool for True {
    type TOption<T> = TSome<T>;

    const VALUE: Option<Self> = Some(True);

    #[inline]
    fn then_some<T>(value: T) -> Option<Self::TOption<T>> {
        Some(TSome(value))
    }

    #[inline]
    fn then_some_ref<T>(value: &T) -> Option<&Self::TOption<T>> {
        Some(TSome::wrap_ref(value))
    }
}

impl<T> TOption<T> for TSome<T> {
    type IsSome = True;

    #[inline]
    fn is_some(&self) -> Self::IsSome {
        True
    }

    #[inline]
    fn get(&self) -> &T {
        &self.0
    }
}

impl TBool for False {
    type TOption<T> = TNone<T>;

    const VALUE: Option<Self> = None;

    #[inline]
    fn then_some<T>(_: T) -> Option<Self::TOption<T>> {
        None
    }

    #[inline]
    fn then_some_ref<T>(_: &T) -> Option<&Self::TOption<T>> {
        None
    }
}

impl<T> TOption<T> for TNone<T> {
    type IsSome = False;

    fn is_some(&self) -> Self::IsSome {
        match self.0 {}
    }

    fn get(&self) -> &T {
        match self.0 {}
    }
}
