use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Debug;

/// A structure, that performs topological sorting over
/// a set of dependencies between items of type T.
#[derive(Clone)]
pub struct TopoSort<T: Eq + std::hash::Hash + Copy> {
    /// Maps each node to its set of dependencies (nodes it depends on)
    dependencies: HashMap<T, HashSet<T>>,
}

impl<T: Debug> Debug for TopoSort<T>
where
    T: Eq + std::hash::Hash + Copy,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self { dependencies } = self;

        f.debug_struct("TopoSort")
            .field("dependencies", dependencies)
            .finish()
    }
}

impl<T: Eq + std::hash::Hash + Copy> Default for TopoSort<T> {
    fn default() -> Self {
        Self {
            dependencies: HashMap::new(),
        }
    }
}

impl<T: Eq + std::hash::Hash + Copy> TopoSort<T> {
    /// Creates a new empty TopoSort.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a dependency: `element` depends on `depends_on`.
    /// This means `depends_on` must come before `element` in the sorted output.
    pub fn add_dependency(&mut self, element: T, depends_on: T) {
        self.dependencies
            .entry(element)
            .or_default()
            .insert(depends_on);
    }

    pub fn clear(&mut self) {
        self.dependencies.clear();
    }

    /// This function takes a list of elements and returns them sorted according to the dependencies.
    /// List of elements might include new points not present in the dependency graph,
    /// as well as omit some points present in the dependency graph.
    ///
    /// If point depends on something not present in the input list, that dependency is ignored.
    pub fn sort_elements(&self, elements: &[T]) -> TopoSortIter<T> {
        TopoSortIter::new(&self.dependencies, elements)
    }
}

pub struct TopoSortIter<T: Eq + std::hash::Hash + Copy> {
    /// Maps each node to its remaining dependency count
    in_degree: HashMap<T, usize>,
    /// Maps each node to nodes that depend on it (reverse index)
    dependents: HashMap<T, Vec<T>>,
    /// Queue of nodes ready to be emitted (no remaining dependencies)
    ready: VecDeque<T>,
}

impl<T: Eq + std::hash::Hash + Copy> TopoSortIter<T> {
    /// Collect rest of the elements into unordered array.
    /// Useful, in case of circular dependency which can't be resolved.
    pub fn into_unordered_vec(self) -> Vec<T> {
        // Include all currently ready nodes (in-degree == 0)
        let mut result: Vec<T> = self.ready.into_iter().collect();
        // Include nodes that still have non-zero in-degree (part of a cycle)
        for (node, count) in self.in_degree {
            if count != 0 {
                result.push(node);
            }
        }
        result
    }
}

impl<T: Eq + std::hash::Hash + Copy> TopoSortIter<T> {
    fn new(dependencies: &HashMap<T, HashSet<T>>, all_nodes: &[T]) -> Self {
        // Build in-degree count and reverse dependency index
        let mut in_degree: HashMap<T, usize> = HashMap::with_capacity(all_nodes.len());
        let mut dependents: HashMap<T, Vec<T>> = HashMap::with_capacity(all_nodes.len());

        // Initialize all nodes with 0 in-degree
        for node in all_nodes {
            in_degree.insert(*node, 0);
        }

        // Build the in-degree counts and reverse index
        for (node, deps) in dependencies {
            if !in_degree.contains_key(node) {
                // Ignore nodes, which are not in the provided all_nodes list
                continue;
            }
            let mut number_of_deps = 0;
            for dep in deps {
                if !in_degree.contains_key(dep) {
                    // Ignore dependencies, which are not in the provided all_nodes list
                    continue;
                }
                dependents.entry(*dep).or_default().push(*node);
                number_of_deps += 1;
            }
            in_degree.insert(*node, number_of_deps);
        }

        // Find all nodes with no dependencies (in-degree == 0)
        let ready: VecDeque<T> = all_nodes
            .iter()
            .copied()
            .filter(|node| in_degree.get(node).copied().unwrap_or(0) == 0)
            .collect();

        Self {
            in_degree,
            dependents,
            ready,
        }
    }
}

impl<T: Eq + std::hash::Hash + Copy> Iterator for TopoSortIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let node = self.ready.pop_front()?;

        // For each node that depends on the current node, decrement its in-degree
        if let Some(deps) = self.dependents.remove(&node) {
            for dependent in deps {
                if let Some(count) = self.in_degree.get_mut(&dependent) {
                    *count -= 1;
                    if *count == 0 {
                        self.ready.push_back(dependent);
                    }
                }
            }
        }

        Some(node)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty() {
        let topo: TopoSort<i32> = TopoSort::new();
        let elements = [1, 2, 3, 4, 5];
        let result: Vec<_> = topo.sort_elements(&elements).collect();
        assert_eq!(result, elements);
    }

    #[test]
    fn test_single_dependency() {
        let mut topo = TopoSort::new();
        let elements = [2, 1];
        topo.add_dependency(2, 1); // 2 depends on 1, so 1 comes first
        let result: Vec<_> = topo.sort_elements(&elements).collect();
        assert_eq!(result, vec![1, 2]);
    }

    #[test]
    fn test_chain() {
        let mut topo = TopoSort::new();
        let elements = [3, 2, 1];
        topo.add_dependency(3, 2); // 3 depends on 2
        topo.add_dependency(2, 1); // 2 depends on 1
        let result: Vec<_> = topo.sort_elements(&elements).collect();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[test]
    fn test_diamond() {
        let mut topo = TopoSort::new();

        let elements = [4, 2, 3, 1];
        topo.add_dependency(4, 2); // 4 depends on 2
        topo.add_dependency(4, 3); // 4 depends on 3
        topo.add_dependency(2, 1); // 2 depends on 1
        topo.add_dependency(3, 1); // 3 depends on 1
        let result: Vec<_> = topo.sort_elements(&elements).collect();

        // 1 must come first, 4 must come last
        assert_eq!(result[0], 1);
        assert_eq!(result[3], 4);
        // 2 and 3 can be in any order
        assert!(result.contains(&2));
        assert!(result.contains(&3));
    }

    #[test]
    fn circular_dependency() {
        // A B C D
        // A <- B <- C
        //   <- C <- D
        //   <- D <- B
        let elements = ['A', 'B', 'C', 'D'];

        let mut topo = TopoSort::new();
        topo.add_dependency('B', 'A');
        topo.add_dependency('C', 'A');
        topo.add_dependency('D', 'A');

        topo.add_dependency('C', 'B');
        topo.add_dependency('D', 'C');
        topo.add_dependency('B', 'D');

        let mut iter = topo.sort_elements(&elements);
        let first = iter.next().unwrap();
        assert_eq!(first, 'A');

        let second = iter.next();
        assert!(second.is_none());

        let unordered = iter.into_unordered_vec();
        assert_eq!(unordered.len(), 3);

        assert!(unordered.contains(&'B'));
        assert!(unordered.contains(&'C'));
        assert!(unordered.contains(&'D'));
    }

    #[test]
    fn test_missing_dependencies() {
        let mut topo = TopoSort::new();
        let elements = [3, 2, 1];
        topo.add_dependency(3, 4); // This one is extra and should be ignored
        topo.add_dependency(2, 1);
        topo.add_dependency(3, 2);
        let result: Vec<_> = topo.sort_elements(&elements).collect();
        // 4 is missing, so 3 should be treated as having no dependencies
        assert_eq!(result, vec![1, 2, 3]);
    }
}
