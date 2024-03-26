use common::types::{Float, ScoreType};
use rand::distributions::{Distribution, Standard};
use rand::Rng;
use schemars::{gen::SchemaGenerator, schema::Schema, JsonSchema};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::iter::Sum;
use std::ops::{Add, AddAssign, Deref, DerefMut, Div, Mul, Neg, Sub};

pub type VectorElementTypeByte = u8;

/// Type of vector element.
///
/// repr(transparent) is required to preserve memory layout
#[repr(transparent)]
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Deserialize, Serialize)]
pub struct VectorElementType<T: Float = f32>(T);

impl<T: Float + JsonSchema> JsonSchema for VectorElementType<T> {
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

impl<T> Distribution<VectorElementType<T>> for Standard
where
    T: Float,
    Standard: Distribution<T>,
{
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> VectorElementType<T> {
        VectorElementType(rng.gen())
    }
}

impl VectorElementType<f32> {
    pub const EPSILON: Self = VectorElementType(f32::EPSILON);
    pub const NEG_INFINITY: Self = VectorElementType(f32::NEG_INFINITY);
}

impl Into<VectorElementTypeByte> for VectorElementType<f32> {
    fn into(self) -> VectorElementTypeByte {
        self.0 as VectorElementTypeByte
    }
}

impl<T: Float> Into<ScoreType<T>> for VectorElementType<T> {
    fn into(self) -> ScoreType<T> {
        ScoreType::from(self.0)
    }
}

// To FFI
pub fn to_primitives_slice(slice: &[VectorElementType<f32>]) -> &[f32] {
    // Safety: VectorElementType has repr(transparent) and
    // contains only a sinlge non-zero-sized field
    unsafe { &*(slice as *const [VectorElementType] as *const [f32]) }
}

impl<T: Float> VectorElementType<T> {
    pub fn min_value() -> Self {
        VectorElementType(T::min_value())
    }
}

impl<T: Float> From<T> for VectorElementType<T> {
    fn from(number: T) -> VectorElementType<T> {
        VectorElementType(number)
    }
}

impl From<i32> for VectorElementType<f32> {
    fn from(integer: i32) -> VectorElementType<f32> {
        VectorElementType(integer as f32)
    }
}

impl<T: Float> Deref for VectorElementType<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Float> DerefMut for VectorElementType<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: Float> Neg for VectorElementType<T> {
    type Output = Self;

    fn neg(self) -> Self::Output {
        Self::from(-(*self))
    }
}

impl<T: Float> Add for VectorElementType<T> {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self(self.0 + other.0)
    }
}

impl<'a, 'b, T: Float> Add<&'b VectorElementType<T>> for &'a VectorElementType<T> {
    type Output = VectorElementType<T>;

    fn add(self, rhs: &'b VectorElementType<T>) -> VectorElementType<T> {
        VectorElementType(self.0 + rhs.0)
    }
}

impl<T: Float> AddAssign for VectorElementType<T> {
    fn add_assign(&mut self, other: Self) {
        *self = Self(self.0 + other.0);
    }
}

impl<T: Float> Sub for VectorElementType<T> {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        Self(self.0 - other.0)
    }
}

impl<'a, 'b, T: Float> Sub<&'b VectorElementType<T>> for &'a VectorElementType<T> {
    type Output = VectorElementType<T>;

    fn sub(self, rhs: &'b VectorElementType<T>) -> VectorElementType<T> {
        VectorElementType(self.0 - rhs.0)
    }
}

impl<T: Float> Mul for VectorElementType<T> {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self {
        Self(self.0 * rhs.0)
    }
}

impl<'a, 'b, T: Float> Mul<&'b VectorElementType<T>> for &'a VectorElementType<T> {
    type Output = VectorElementType<T>;

    fn mul(self, rhs: &'b VectorElementType<T>) -> VectorElementType<T> {
        VectorElementType(self.0 * rhs.0)
    }
}

impl<T: Float> Div for VectorElementType<T> {
    type Output = Self;

    fn div(self, rhs: Self) -> Self {
        Self(self.0 / rhs.0)
    }
}

impl<T: Float> Sum for VectorElementType<T> {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        Self::from(iter.fold(T::zero(), |a, b| a + *b))
    }
}

impl<T: Float> Sum<T> for VectorElementType<T> {
    fn sum<I: Iterator<Item = T>>(iter: I) -> Self {
        Self::from(iter.fold(T::zero(), |a, b| a + b))
    }
}

// Compatibility layer for gRPC types
pub fn to_primitives_vec(vector: Vec<VectorElementType>) -> Vec<f32> {
    // no new allocation - check https://rust.godbolt.org/z/hqsnqeG3Y
    vector.into_iter().map(|e| *e).collect()
}

pub fn from_primitives_vec(vector: Vec<f32>) -> Vec<VectorElementType> {
    vector.into_iter().map(|e| e.into()).collect()
}
