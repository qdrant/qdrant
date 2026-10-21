/// Where a term's frequencies lie, relative to the start of the frequency
/// data of `term_frequencies.dat`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrequencySpan {
    /// Frequencies in the list: one per posting.
    pub len: usize,
    pub offset: u64,
    pub size: u64,
}
