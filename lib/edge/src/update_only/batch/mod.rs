//! Folding a batch of update operations into per-point work.
//!
//! All operations touching the same point collapse into one [`PointUpdates`]:
//! a point is written at most once, however many operations named it, and read
//! only if some surviving mutation needs the stored point — a batch that
//! upserts a point never reads it.
//!
//! This stage is pure: nothing here touches storage.

mod mutation;
mod plan;
#[cfg(test)]
mod tests;

pub use self::mutation::PointUpdates;
pub use self::plan::UpdateBatchPlan;
