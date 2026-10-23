//! MaxSim over TurboQuant multivector records, shared by the writable and the
//! read-only storage: both score through [`TurboQuantizer::score_multi_query`]
//! and [`TurboQuantizer::score_symmetric_max_sim`].
//!
//! [`TurboQuantizer::score_multi_query`]: quantization::turboquant::quantization::TurboQuantizer::score_multi_query
//! [`TurboQuantizer::score_symmetric_max_sim`]: quantization::turboquant::quantization::TurboQuantizer::score_symmetric_max_sim

use quantization::MultiQuery;
use quantization::turboquant::EncodedQueryTQ;

/// A multi-vector query prepared for a TurboQuant multivector storage.
pub type TurboMultiQuery = MultiQuery<EncodedQueryTQ>;
