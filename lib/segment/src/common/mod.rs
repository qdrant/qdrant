pub mod arc_atomic_ref_cell_iterator;
pub mod error_logging;
pub mod file_operations;
pub mod rocksdb_operations;
pub mod utils;
pub mod version;

use crate::entry::entry_point::OperationResult;

pub type Flusher = Box<dyn FnOnce() -> OperationResult<()> + Send>;
