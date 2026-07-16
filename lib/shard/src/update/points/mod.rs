//! Point-level operations: upserts (plain, conditional and raw), deletes and syncs.

mod delete;
mod sync;
mod upsert;

pub use self::delete::{delete_points, delete_points_by_filter};
pub use self::sync::{sync_points, sync_points_raw};
pub(crate) use self::upsert::retain_conditional_upsert_points;
pub use self::upsert::{conditional_upsert, upsert_points, upsert_points_raw};
