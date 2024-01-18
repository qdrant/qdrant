#[allow(dead_code)]
mod immutable_inverted_index;
#[allow(dead_code)]
mod immutable_text_index;
mod inverted_index;
mod posting_list;
mod postings_iterator;
pub mod text_index;
mod tokenizers;

#[cfg(test)]
mod tests;
