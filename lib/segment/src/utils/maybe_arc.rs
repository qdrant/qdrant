use std::ops::Deref;
use std::sync::Arc;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// Structure that acts as `T` most of the time but allows to interchange being wrapped within an `Arc` or not.
// This is helpful, when a variable can become memory-intensive but must remain the ability to get cloned.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(untagged)] // Make this type transparent when de/serializing and always deserialize as `NoArc`, since it's the first enum kind that matches.
pub enum MaybeArc<T> {
    NoArc(T),
    Arc(Arc<T>),
}

impl<T> MaybeArc<T> {
    #[inline]
    pub fn arc(t: T) -> Self {
        Self::Arc(Arc::new(t))
    }

    /// Create a new `MaybeArc` wrapper that doesn't use an `Arc` internally.
    #[inline]
    pub fn no_arc(t: T) -> Self {
        Self::NoArc(t)
    }
}

impl<T: Clone> MaybeArc<T> {
    /// Converts the `MaybeArc` back to `T`, potentially cloning the inner vaule
    /// in case it's an `Arc` that has existing references.
    #[inline]
    pub fn into_inner(self) -> T {
        match self {
            Self::Arc(a) => Arc::unwrap_or_clone(a),
            Self::NoArc(a) => a,
        }
    }
}

impl<T> Deref for MaybeArc<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Arc(a) => a,
            Self::NoArc(a) => a,
        }
    }
}

impl<T, I> FromIterator<I> for MaybeArc<T>
where
    T: FromIterator<I>,
{
    fn from_iter<U: IntoIterator<Item = I>>(iter: U) -> Self {
        let inner = T::from_iter(iter);

        // Using `NoArc` as default implementation to stay as close as possible to the type `T` and
        // don't accidentally introducing overhead.
        // A caller can always manually create the `MaybeArc` if using an `Arc` is preferred.
        MaybeArc::NoArc(inner)
    }
}

#[cfg(test)]
mod test {
    // use super::*;

    //
}
