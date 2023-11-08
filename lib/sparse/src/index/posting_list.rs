use std::cmp::max;

use common::types::PointOffsetType;
use ordered_float::OrderedFloat;

use crate::common::types::DimWeight;

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct PostingElement {
    /// Record ID
    pub record_id: PointOffsetType,
    /// Weight of the record in the dimension
    pub weight: DimWeight,
    /// Max weight of the next elements in the posting list.
    pub max_next_weight: DimWeight,
}

const DEFAULT_MAX_NEXT_WEIGHT: DimWeight = f32::NEG_INFINITY;

impl PostingElement {
    /// Initialize negative infinity as max_next_weight.
    /// Needs to be updated at insertion time.
    pub(crate) fn new(record_id: PointOffsetType, weight: DimWeight) -> PostingElement {
        PostingElement {
            record_id,
            weight,
            max_next_weight: DEFAULT_MAX_NEXT_WEIGHT,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct PostingList {
    /// List of the posting elements ordered by id
    pub elements: Vec<PostingElement>,
}

impl PostingList {
    /// used for testing
    pub fn from(records: Vec<(PointOffsetType, DimWeight)>) -> PostingList {
        let mut posting_list = PostingBuilder::new();
        for (id, weight) in records {
            posting_list.add(id, weight);
        }
        posting_list.build()
    }

    /// Creates a new posting list with a single element.
    pub fn new_one(record_id: PointOffsetType, weight: DimWeight) -> PostingList {
        PostingList {
            elements: vec![PostingElement::new(record_id, weight)],
        }
    }

    /// Upsert a posting element into the posting list.
    ///
    /// Worst case is adding a new element at the end of the list with a very large weight.
    /// This forces to propagate it as potential max_next_weight to all the previous elements.
    pub fn upsert(&mut self, posting_element: PostingElement) {
        // find insertion point in sorted posting list
        let index = self
            .elements
            .binary_search_by_key(&posting_element.record_id, |e| e.record_id);

        let modified_index = match index {
            Ok(found_index) => {
                // Update existing element for the same id
                let element = &mut self.elements[found_index];
                if element.weight == posting_element.weight {
                    // no need to update anything
                    None
                } else {
                    // the structure of the posting list is not changed, no need to update max_next_weight
                    element.weight = posting_element.weight;
                    Some(found_index)
                }
            }
            Err(insert_index) => {
                // Insert new element by shifting elements to the right
                self.elements.insert(insert_index, posting_element);
                // the structure of the posting list is changed, need to update max_next_weight
                if insert_index == self.elements.len() - 1 {
                    // inserted at the end
                    Some(insert_index)
                } else {
                    // inserted in the middle - need to propagated max_next_weight from the right
                    Some(insert_index + 1)
                }
            }
        };
        // Propagate max_next_weight update to the previous entries
        if let Some(modified_index) = modified_index {
            self.propagate_max_next_weight_to_the_left(modified_index);
        }
    }

    /// Propagates `max_next_weight` from the entry at `up_to_index` to previous entries.
    /// If an entry has a weight larger than `max_next_weight`, the propagation stops.
    fn propagate_max_next_weight_to_the_left(&mut self, up_to_index: usize) {
        // used element at `up_to_index` as the starting point
        let starting_element = &self.elements[up_to_index];
        let mut max_next_weight = max(
            OrderedFloat(starting_element.max_next_weight),
            OrderedFloat(starting_element.weight),
        )
        .0;

        // propagate max_next_weight update to the previous entries
        for element in self.elements[..up_to_index].iter_mut().rev() {
            // update max_next_weight for element
            element.max_next_weight = max_next_weight;
            if element.weight >= max_next_weight {
                // no need to propagate further because the current element is larger
                break;
            } else {
                // update max_next_weight based on current element
                max_next_weight = max_next_weight.max(element.weight);
            }
        }
    }
}

pub struct PostingBuilder {
    elements: Vec<PostingElement>,
}

impl Default for PostingBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl PostingBuilder {
    pub fn new() -> PostingBuilder {
        PostingBuilder {
            elements: Vec::new(),
        }
    }

    pub fn add(&mut self, record_id: PointOffsetType, weight: DimWeight) {
        self.elements.push(PostingElement::new(record_id, weight));
    }

    pub fn build(mut self) -> PostingList {
        // Sort by id
        self.elements.sort_unstable_by_key(|e| e.record_id);

        // Check for duplicates
        #[cfg(debug_assertions)]
        {
            if let Some(e) = self
                .elements
                .windows(2)
                .find(|e| e[0].record_id == e[1].record_id)
            {
                panic!("Duplicate id {} in posting list", e[0].record_id);
            }
        }

        // Calculate max_next_weight
        let mut max_next_weight = f32::NEG_INFINITY;
        for element in self.elements.iter_mut().rev() {
            element.max_next_weight = max_next_weight;
            max_next_weight = max_next_weight.max(element.weight);
        }

        PostingList {
            elements: self.elements,
        }
    }
}

/// Iterator over posting list elements offering skipping abilities to avoid full iteration.
pub struct PostingListIterator<'a> {
    pub elements: &'a [PostingElement],
    current_index: usize,
}

impl<'a> Iterator for PostingListIterator<'a> {
    type Item = &'a PostingElement;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_index < self.elements.len() {
            let element = &self.elements[self.current_index];
            self.current_index += 1;
            Some(element)
        } else {
            None
        }
    }
}

impl<'a> PostingListIterator<'a> {
    pub fn new(elements: &'a [PostingElement]) -> PostingListIterator<'a> {
        PostingListIterator {
            elements,
            current_index: 0,
        }
    }

    /// Returns the next element without advancing the iterator.
    pub fn peek(&self) -> Option<&PostingElement> {
        self.elements.get(self.current_index)
    }

    /// Returns the number of elements from the current position to the end of the list.
    pub fn len_to_end(&self) -> usize {
        self.elements.len() - self.current_index
    }

    /// Tries to find the element with ID == id and returns it.
    /// If the element is not found, the iterator is advanced to the next element with ID > id
    /// and None is returned.
    /// If the iterator is already at the end, None is returned.
    /// If the iterator skipped to the end, None is returned and current index is set to the length of the list.
    /// Uses binary search.
    pub fn skip_to(&mut self, id: PointOffsetType) -> Option<&PostingElement> {
        // Check if we are already at the end
        if self.current_index >= self.elements.len() {
            return None;
        }

        // Use binary search to find the next element with ID > id
        let next_element =
            self.elements[self.current_index..].binary_search_by(|e| e.record_id.cmp(&id));

        match next_element {
            Ok(found_offset) => {
                self.current_index += found_offset;
                Some(&self.elements[self.current_index])
            }
            Err(insert_index) => {
                self.current_index += insert_index;
                None
            }
        }
    }

    /// Skips to the end of the posting list and returns None.
    pub fn skip_to_end(&mut self) -> Option<&PostingElement> {
        self.current_index = self.elements.len();
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_posting_operations() {
        let mut builder = PostingBuilder::new();
        builder.add(1, 1.0);
        builder.add(2, 2.1);
        builder.add(5, 5.0);
        builder.add(3, 2.0);
        builder.add(8, 3.4);
        builder.add(10, 3.0);
        builder.add(20, 3.0);
        builder.add(7, 4.0);
        builder.add(11, 3.0);

        let posting_list = builder.build();

        let mut iter = PostingListIterator::new(&posting_list.elements);

        assert_eq!(iter.peek().unwrap().record_id, 1);

        assert_eq!(iter.next().unwrap().record_id, 1);
        assert_eq!(iter.peek().unwrap().record_id, 2);
        assert_eq!(iter.next().unwrap().record_id, 2);
        assert_eq!(iter.peek().unwrap().record_id, 3);

        assert_eq!(iter.skip_to(7).unwrap().record_id, 7);
        assert_eq!(iter.peek().unwrap().record_id, 7);

        assert!(iter.skip_to(9).is_none());
        assert_eq!(iter.peek().unwrap().record_id, 10);

        assert!(iter.skip_to(20).is_some());
        assert_eq!(iter.peek().unwrap().record_id, 20);

        assert!(iter.skip_to(21).is_none());
        assert!(iter.peek().is_none());
    }

    #[test]
    fn test_upsert_insert_last() {
        let mut builder = PostingBuilder::new();
        builder.add(1, 1.0);
        builder.add(3, 3.0);
        builder.add(2, 2.0);

        let mut posting_list = builder.build();

        // sorted by id
        assert_eq!(posting_list.elements[0].record_id, 1);
        assert_eq!(posting_list.elements[0].weight, 1.0);
        assert_eq!(posting_list.elements[0].max_next_weight, 3.0);

        assert_eq!(posting_list.elements[1].record_id, 2);
        assert_eq!(posting_list.elements[1].weight, 2.0);
        assert_eq!(posting_list.elements[1].max_next_weight, 3.0);

        assert_eq!(posting_list.elements[2].record_id, 3);
        assert_eq!(posting_list.elements[2].weight, 3.0);
        assert_eq!(
            posting_list.elements[2].max_next_weight,
            DEFAULT_MAX_NEXT_WEIGHT
        );

        // insert mew last element
        posting_list.upsert(PostingElement::new(4, 4.0));
        assert_eq!(posting_list.elements[3].record_id, 4);
        assert_eq!(posting_list.elements[3].weight, 4.0);
        assert_eq!(
            posting_list.elements[3].max_next_weight,
            DEFAULT_MAX_NEXT_WEIGHT
        );

        // must update max_next_weight of previous elements if necessary
        for element in posting_list.elements.iter().take(3) {
            assert_eq!(element.max_next_weight, 4.0);
        }
    }

    #[test]
    fn test_upsert_insert_in_gap() {
        let mut builder = PostingBuilder::new();
        builder.add(1, 1.0);
        builder.add(3, 3.0);
        builder.add(2, 2.0);
        // no entry for 4
        builder.add(5, 5.0);

        let mut posting_list = builder.build();

        // sorted by id
        assert_eq!(posting_list.elements[0].record_id, 1);
        assert_eq!(posting_list.elements[0].weight, 1.0);
        assert_eq!(posting_list.elements[0].max_next_weight, 5.0);

        assert_eq!(posting_list.elements[1].record_id, 2);
        assert_eq!(posting_list.elements[1].weight, 2.0);
        assert_eq!(posting_list.elements[1].max_next_weight, 5.0);

        assert_eq!(posting_list.elements[2].record_id, 3);
        assert_eq!(posting_list.elements[2].weight, 3.0);
        assert_eq!(posting_list.elements[2].max_next_weight, 5.0);

        assert_eq!(posting_list.elements[3].record_id, 5);
        assert_eq!(posting_list.elements[3].weight, 5.0);
        assert_eq!(
            posting_list.elements[3].max_next_weight,
            DEFAULT_MAX_NEXT_WEIGHT
        );

        // insert mew last element
        posting_list.upsert(PostingElement::new(4, 4.0));

        // `4` is shifted to the right
        assert_eq!(posting_list.elements[4].record_id, 5);
        assert_eq!(posting_list.elements[4].weight, 5.0);
        assert_eq!(
            posting_list.elements[4].max_next_weight,
            DEFAULT_MAX_NEXT_WEIGHT
        );

        // new element
        assert_eq!(posting_list.elements[3].record_id, 4);
        assert_eq!(posting_list.elements[3].weight, 4.0);

        // must update max_next_weight of previous elements
        for element in posting_list.elements.iter().take(4) {
            assert_eq!(element.max_next_weight, 5.0);
        }
    }

    #[test]
    fn test_upsert_update() {
        let mut builder = PostingBuilder::new();
        builder.add(1, 1.0);
        builder.add(3, 3.0);
        builder.add(2, 2.0);

        let mut posting_list = builder.build();

        // sorted by id
        assert_eq!(posting_list.elements[0].record_id, 1);
        assert_eq!(posting_list.elements[0].weight, 1.0);
        assert_eq!(posting_list.elements[0].max_next_weight, 3.0);

        assert_eq!(posting_list.elements[1].record_id, 2);
        assert_eq!(posting_list.elements[1].weight, 2.0);
        assert_eq!(posting_list.elements[1].max_next_weight, 3.0);

        assert_eq!(posting_list.elements[2].record_id, 3);
        assert_eq!(posting_list.elements[2].weight, 3.0);
        assert_eq!(
            posting_list.elements[2].max_next_weight,
            DEFAULT_MAX_NEXT_WEIGHT
        );

        // increase weight of existing element
        posting_list.upsert(PostingElement::new(2, 4.0));

        assert_eq!(posting_list.elements[0].record_id, 1);
        assert_eq!(posting_list.elements[0].weight, 1.0);
        assert_eq!(posting_list.elements[0].max_next_weight, 4.0); // update propagated

        assert_eq!(posting_list.elements[1].record_id, 2);
        assert_eq!(posting_list.elements[1].weight, 4.0); // updated
        assert_eq!(posting_list.elements[1].max_next_weight, 3.0);

        assert_eq!(posting_list.elements[2].record_id, 3);
        assert_eq!(posting_list.elements[2].weight, 3.0);
        assert_eq!(
            posting_list.elements[2].max_next_weight,
            DEFAULT_MAX_NEXT_WEIGHT
        );
    }
}
