use std::marker::PhantomData;

use crate::types::PointOffsetType;

/// A check that tests whether points satisfy a condition.
pub trait ConditionChecker {
    type Error;

    fn check(&self, point_id: PointOffsetType) -> Result<bool, Self::Error>;

    /// Same as [`Self::check`] but ignoring errors.
    fn check_infallible(&self, point_id: PointOffsetType) -> bool {
        // This method is a workaround to keep the performance on-par.
        // It's faster to do `.unwrap_or(false)` *inside* the trait method
        // because the compiler can't inline `&dyn Trait` methods.
        //
        // TODO(uio): remove this method and handle errors properly.
        self.check(point_id).unwrap_or(false)
    }
}

/// A checker that ignores the point and always returns the same value.
pub struct ConstantConditionChecker<E>(bool, PhantomData<E>);

impl<E> ConstantConditionChecker<E> {
    pub const MATCH_NONE: Self = Self(false, PhantomData);

    pub const MATCH_ALL: Self = Self(true, PhantomData);

    pub const fn new(value: bool) -> Self {
        ConstantConditionChecker(value, PhantomData)
    }
}

impl<E> ConditionChecker for ConstantConditionChecker<E> {
    type Error = E;

    fn check(&self, _point_id: PointOffsetType) -> Result<bool, E> {
        Ok(self.0)
    }
}
