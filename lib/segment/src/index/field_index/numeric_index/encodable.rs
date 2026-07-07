//! The [`Encodable`] bound shared by every numeric-index storage variant's
//! key type.

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::types::{DateTimePayloadType, FloatPayloadType, IntPayloadType};

pub trait Encodable: Copy + Serialize + DeserializeOwned + 'static {}

impl Encodable for IntPayloadType {}

impl Encodable for u128 {}

impl Encodable for FloatPayloadType {}

impl Encodable for DateTimePayloadType {}
