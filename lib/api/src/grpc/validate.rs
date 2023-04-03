use validator::{HasLen, Validate, ValidationErrors};

pub trait ValidateExt {
    fn validate(&self) -> Result<(), ValidationErrors>;
}

impl Validate for dyn ValidateExt {
    fn validate(&self) -> Result<(), ValidationErrors> {
        ValidateExt::validate(self)
    }
}

pub trait HasLenExt {
    fn length(&self) -> u64;
}

impl HasLen for dyn HasLenExt {
    fn length(&self) -> u64 {
        HasLenExt::length(self)
    }
}

impl<V> ValidateExt for &::core::option::Option<V>
where
    V: Validate,
{
    fn validate(&self) -> Result<(), ValidationErrors> {
        self.as_ref().map(Validate::validate).unwrap_or(Ok(()))
    }
}

impl<V> ValidateExt for ::core::option::Option<V>
where
    V: Validate,
{
    fn validate(&self) -> Result<(), ValidationErrors> {
        self.as_ref().map(Validate::validate).unwrap_or(Ok(()))
    }
}

impl HasLenExt for ::prost::alloc::string::String {
    fn length(&self) -> u64 {
        HasLen::length(self)
    }
}

impl<V> ValidateExt for Vec<V>
where
    V: Validate,
{
    fn validate(&self) -> Result<(), ValidationErrors> {
        let errors = self
            .iter()
            .enumerate()
            .filter_map(|(i, v)| v.validate().err().map(|err| (i, err)))
            .fold(ValidationErrors::new(), |a, (_i, b)| {
                ValidationErrors::merge(Err(a), "", Err(b)).unwrap_err()
            });
        errors.errors().is_empty().then_some(()).ok_or(errors)
    }
}
