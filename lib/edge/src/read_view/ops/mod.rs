//! Per-operation read implementations on [`EdgeReadView`](crate::read_view::EdgeReadView): one
//! file per operation, all running over the same consistent segment snapshot.

mod count;
mod facet;
mod grouping;
mod info;
mod matrix;
mod query;
mod retrieve;
mod scroll;
mod search;

pub use self::grouping::{Group, GroupRequest};
pub use self::info::ShardInfo;
pub use self::matrix::{SearchMatrixRequest, SearchMatrixResponse};
