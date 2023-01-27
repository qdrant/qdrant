use std::fmt;

pub trait Validator<T> {
    type Error: fmt::Display;

    fn validate(value: &T) -> Result<(), Self::Error>;

    fn deserialize<'de, D>(deserializer: D) -> Result<T, D::Error>
    where
        D: serde::Deserializer<'de>,
        T: serde::Deserialize<'de>,
    {
        let value = T::deserialize(deserializer)?;
        Self::validate(&value).map_err(serde::de::Error::custom)?;
        Ok(value)
    }
}

pub enum NonZero {}

impl Validator<usize> for NonZero {
    type Error = &'static str;

    fn validate(&value: &usize) -> Result<(), Self::Error> {
        if value != 0 {
            Ok(())
        } else {
            Err("expected a non-zero value")
        }
    }
}
