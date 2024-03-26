use num_traits::{float::FloatCore, FromPrimitive};
use ordered_float::OrderedFloat;
use schemars::{gen::SchemaGenerator, schema::Schema, JsonSchema};
use serde::{Deserialize, Serialize};
use std::any::type_name;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::fmt;
use std::iter::Sum;
use std::ops::{Add, AddAssign, Deref, DerefMut, Div, Mul, Neg, Sub};

pub trait Float: FloatCore {}

impl Float for f32 {}

/// Type of vector matching score
#[repr(transparent)]
#[derive(Copy, Clone, PartialEq, Debug, Default, Deserialize, Serialize)]
pub struct ScoreType<T: Float = f32>(T);

impl fmt::Display for ScoreType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<T: Float + JsonSchema> JsonSchema for ScoreType<T> {
    fn schema_name() -> String {
        T::schema_name()
    }

    fn schema_id() -> Cow<'static, str> {
        T::schema_id()
    }

    fn json_schema(gen: &mut SchemaGenerator) -> Schema {
        T::json_schema(gen)
    }
}

impl ScoreType<f32> {
    pub const EPSILON: Self = ScoreType(f32::EPSILON);
    pub const NEG_INFINITY: Self = ScoreType(f32::NEG_INFINITY);
}

impl<T: Float> ScoreType<T> {
    pub fn min_value() -> Self {
        ScoreType(T::min_value())
    }

    pub fn max_value() -> Self {
        ScoreType(T::max_value())
    }
}

impl<T: Float> From<T> for ScoreType<T> {
    fn from(number: T) -> ScoreType<T> {
        ScoreType(number)
    }
}

impl<T: Float + FromPrimitive> From<i32> for ScoreType<T> {
    fn from(number: i32) -> ScoreType<T> {
        ScoreType(
            T::from(number).unwrap_or_else(|| {
                panic!("i32 {} cannot be casted to {}", number, type_name::<T>())
            }),
        )
    }
}

impl<T: Float> Deref for ScoreType<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Float> DerefMut for ScoreType<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: Float> Eq for ScoreType<T> {}

impl<T: Float> Ord for ScoreType<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        OrderedFloat(self.0).cmp(&OrderedFloat(other.0))
    }
}

impl<T: Float> PartialOrd for ScoreType<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Float> Neg for ScoreType<T> {
    type Output = Self;

    fn neg(self) -> Self::Output {
        Self::from(-(*self))
    }
}

impl<T: Float> Add for ScoreType<T> {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self(self.0 + other.0)
    }
}

impl<T: Float> AddAssign for ScoreType<T> {
    fn add_assign(&mut self, other: Self) {
        *self = Self(self.0 + other.0);
    }
}

impl<T: Float> Sub for ScoreType<T> {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        Self(self.0 - other.0)
    }
}

impl<T: Float> Mul for ScoreType<T> {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self {
        Self(self.0 * rhs.0)
    }
}

impl<T: Float> Div for ScoreType<T> {
    type Output = Self;

    fn div(self, rhs: Self) -> Self {
        Self(self.0 / rhs.0)
    }
}

impl<T: Float> Sum for ScoreType<T> {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        Self::from(iter.fold(T::zero(), |a, b| a + *b))
    }
}

impl<T: Float> Sum<T> for ScoreType<T> {
    fn sum<I: Iterator<Item = T>>(iter: I) -> Self {
        Self::from(iter.fold(T::zero(), |a, b| a + b))
    }
}
