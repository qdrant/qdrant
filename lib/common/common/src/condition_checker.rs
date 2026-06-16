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

impl<E, F: Fn(PointOffsetType) -> Result<bool, E>> ConditionChecker for F {
    type Error = E;

    fn check(&self, point_id: PointOffsetType) -> Result<bool, E> {
        self(point_id)
    }
}
