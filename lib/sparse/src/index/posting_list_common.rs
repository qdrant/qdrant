use common::types::PointOffsetType;

use crate::common::types::DimWeight;

pub const DEFAULT_MAX_NEXT_WEIGHT: DimWeight = f32::NEG_INFINITY;

#[derive(Debug, Clone, PartialEq)]
pub struct GenericPostingElement<W> {
    /// Record ID
    pub record_id: PointOffsetType,
    /// Weight of the record in the dimension
    pub weight: W,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PostingElement {
    /// Record ID
    pub record_id: PointOffsetType,
    /// Weight of the record in the dimension
    pub weight: DimWeight,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PostingElementEx {
    /// Record ID
    pub record_id: PointOffsetType,
    /// Weight of the record in the dimension
    pub weight: DimWeight,
    /// Max weight of the next elements in the posting list.
    pub max_next_weight: DimWeight,
}

impl PostingElementEx {
    /// Initialize negative infinity as max_next_weight.
    /// Needs to be updated at insertion time.
    pub(crate) fn new(record_id: PointOffsetType, weight: DimWeight) -> PostingElementEx {
        PostingElementEx {
            record_id,
            weight,
            max_next_weight: DEFAULT_MAX_NEXT_WEIGHT,
        }
    }
}

impl From<PostingElementEx> for PostingElement {
    fn from(element: PostingElementEx) -> PostingElement {
        PostingElement {
            record_id: element.record_id,
            weight: element.weight,
        }
    }
}

pub trait PostingListIter {
    fn peek(&mut self) -> Option<PostingElementEx>;

    fn last_id(&self) -> Option<PointOffsetType>;

    /// Size of the weight element
    fn element_size(&self) -> usize;

    /// Tries to find the element with ID == id and returns it.
    /// If the element is not found, the iterator is advanced to the next element with ID > id
    /// and None is returned.
    /// If the iterator is already at the end, None is returned.
    fn skip_to(&mut self, record_id: PointOffsetType) -> Option<PostingElementEx>;

    fn skip_to_end(&mut self);

    fn len_to_end(&self) -> usize;

    fn current_index(&self) -> usize;

    /// Iterate over the posting list until `id` is reached (inclusive).
    fn for_each_till_id<Ctx: ?Sized>(
        &mut self,
        id: PointOffsetType,
        ctx: &mut Ctx,
        f: impl FnMut(&mut Ctx, PointOffsetType, DimWeight),
    );

    /// Whether the max_next_weight is reliable.
    fn reliable_max_next_weight() -> bool;

    fn into_std_iter(self) -> impl Iterator<Item = PostingElement>;
}
