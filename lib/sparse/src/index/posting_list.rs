use crate::common::types::{DimWeight, RecordId};

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct PostingElement {
    pub record_id: RecordId,
    pub weight: DimWeight,
    pub max_next_weight: DimWeight,
}

#[derive(Debug, Default, Clone)]
pub struct PostingList {
    /// List of the posting elements ordered by id
    pub elements: Vec<PostingElement>,
}

impl PostingList {
    #[cfg(test)]
    pub fn from(records: Vec<(RecordId, DimWeight)>) -> PostingList {
        let mut posting_list = PostingBuilder::new();
        for (id, weight) in records {
            posting_list.add(id, weight);
        }
        posting_list.build()
    }
}

pub struct PostingBuilder {
    elements: Vec<PostingElement>,
}

impl PostingBuilder {
    pub fn new() -> PostingBuilder {
        PostingBuilder {
            elements: Vec::new(),
        }
    }

    pub fn add(&mut self, record_id: RecordId, weight: DimWeight) {
        self.elements.push(PostingElement {
            record_id,
            weight,
            max_next_weight: f32::NEG_INFINITY,
        });
    }

    pub fn build(mut self) -> PostingList {
        // Sort by id
        self.elements.sort_unstable_by_key(|e| e.record_id);

        // Check for duplicates
        #[cfg(debug_assertions)]
        {
            for i in 1..self.elements.len() {
                if self.elements[i].record_id == self.elements[i - 1].record_id {
                    panic!(
                        "Duplicate id {} in posting list",
                        self.elements[i].record_id
                    );
                }
            }
        }

        // Calculate max_next_weight
        let mut max_next_weight = f32::NEG_INFINITY;
        for i in (0..self.elements.len()).rev() {
            let element = &mut self.elements[i];
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
    elements: &'a [PostingElement],
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
    pub fn skip_to(&mut self, id: RecordId) -> Option<&PostingElement> {
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
}
