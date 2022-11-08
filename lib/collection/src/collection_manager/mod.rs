pub mod collection_updater;
pub mod holders;
pub mod optimizers;
pub mod segments_searcher;

mod segments_updater;

#[allow(dead_code)]
pub(crate) mod fixtures;

mod probabilistic_segment_search_sampling;
#[cfg(test)]
mod tests;
