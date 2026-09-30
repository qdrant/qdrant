use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Debug;

/// A structure, that performs topological sorting over
/// a set of dependencies between items of type T.
#[derive(Clone)]
pub struct TopoSort<T: Eq + std::hash::Hash + Copy, V> {
    /// Maps each node to its set of dependencies (nodes it depends on)
    dependencies: HashMap<T, HashMap<T, V>>,
}

impl<T: Debug, V: Debug> Debug for TopoSort<T, V>
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

impl<T: Eq + std::hash::Hash + Copy, V> Default for TopoSort<T, V> {
    fn default() -> Self {
        Self {
            dependencies: HashMap::new(),
        }
    }
}

impl<T: Eq + std::hash::Hash + Copy, V> TopoSort<T, V> {
    /// Creates a new empty TopoSort.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a dependency: `element` depends on `depends_on`.
    /// This means `depends_on` must come before `element` in the sorted output.
    ///
    /// Additionally, stores a value associated with the dependency.
    /// Overwrites any existing value for the same dependency.
    ///
    /// An edge that would close a cycle — including a self-edge — is rejected, because the only
    /// reader is [`Self::sort_elements`], and a topological sort cannot order a cycle: the
    /// elements left over would come back as "unordered" leftovers rather than in dependency
    /// order. Rejecting at insertion keeps the graph acyclic by construction, so ordering stays
    /// total no matter what order edges arrive in.
    ///
    /// Returns whether the edge is in the graph afterwards. A `false` return means the request
    /// was dropped, so the caller must not assume the recorded ordering was applied.
    pub fn add_dependency(&mut self, element: T, depends_on: T, value: V) -> bool {
        if element == depends_on {
            // A self-edge is a cycle of length one, and no order satisfies it.
            log::warn!("Refusing self-referential dependency in topological sort");
            return false;
        }

        if self
            .dependencies
            .get(&element)
            .is_some_and(|deps| deps.contains_key(&depends_on))
        {
            // Edge already recorded: overwriting its value cannot change the graph shape, and
            // this is the common case (a batch of points moving between the same pair of
            // segments), so it skips the reachability walk below.
            self.dependencies
                .entry(element)
                .or_default()
                .insert(depends_on, value);
            return true;
        }

        if self.depends_on(&depends_on, &element) {
            // `depends_on` already (transitively) depends on `element`, so recording
            // `element depends_on depends_on` would make the two contradict each other. The
            // pre-existing edge is kept: dropping the new one is the only way to leave a
            // usable order, and matches what the unordered leftovers used to degrade to.
            log::warn!("Refusing dependency that would create a cycle in topological sort");
            return false;
        }

        self.dependencies
            .entry(element)
            .or_default()
            .insert(depends_on, value);
        true
    }

    /// Whether `element` already depends on `target`, directly or through other elements.
    fn depends_on(&self, element: &T, target: &T) -> bool {
        let mut stack = vec![*element];
        let mut seen = HashSet::with_capacity(self.dependencies.len());
        while let Some(node) = stack.pop() {
            if node == *target {
                return true;
            }
            if !seen.insert(node) {
                continue;
            }
            if let Some(deps) = self.dependencies.get(&node) {
                stack.extend(deps.keys().copied());
            }
        }
        false
    }

    /// Returns the elements `element` depends on, with the value stored per dependency.
    pub fn dependencies_of(&self, element: &T) -> impl Iterator<Item = (&T, &V)> {
        self.dependencies.get(element).into_iter().flatten()
    }

    /// Removes dependencies for which the filter function returns false
    pub fn retain(&mut self, filter: impl Fn(&T, &T, &V) -> bool) {
        self.dependencies.retain(|element, deps| {
            deps.retain(|depends_on, value| filter(element, depends_on, value));
            !deps.is_empty()
        });
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
    fn new<V>(dependencies: &HashMap<T, HashMap<T, V>>, all_nodes: &[T]) -> Self {
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
            for dep in deps.keys() {
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
        let topo: TopoSort<i32, ()> = TopoSort::new();
        let elements = [1, 2, 3, 4, 5];
        let result: Vec<_> = topo.sort_elements(&elements).collect();
        assert_eq!(result, elements);
    }

    #[test]
    fn test_single_dependency() {
        let mut topo = TopoSort::new();
        let elements = [2, 1];
        topo.add_dependency(2, 1, ()); // 2 depends on 1, so 1 comes first
        let result: Vec<_> = topo.sort_elements(&elements).collect();
        assert_eq!(result, vec![1, 2]);
    }

    #[test]
    fn test_chain() {
        let mut topo = TopoSort::new();
        let elements = [3, 2, 1];
        topo.add_dependency(3, 2, ()); // 3 depends on 2
        topo.add_dependency(2, 1, ()); // 2 depends on 1
        let result: Vec<_> = topo.sort_elements(&elements).collect();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[test]
    fn test_diamond() {
        let mut topo = TopoSort::new();

        let elements = [4, 2, 3, 1];
        topo.add_dependency(4, 2, ()); // 4 depends on 2
        topo.add_dependency(4, 3, ()); // 4 depends on 3
        topo.add_dependency(2, 1, ()); // 2 depends on 1
        topo.add_dependency(3, 1, ()); // 3 depends on 1
        let result: Vec<_> = topo.sort_elements(&elements).collect();

        // 1 must come first, 4 must come last
        assert_eq!(result[0], 1);
        assert_eq!(result[3], 4);
        // 2 and 3 can be in any order
        assert!(result.contains(&2));
        assert!(result.contains(&3));
    }

    /// An edge that would close a cycle is rejected, so the graph stays a DAG and
    /// `sort_elements` can always produce a total order.
    #[test]
    fn two_node_cycle_is_rejected() {
        let mut topo: TopoSort<char, ()> = TopoSort::new();

        assert!(topo.add_dependency('A', 'B', ()));
        // 'B' already depends on 'A', so 'A' depending on 'B' contradicts it.
        assert!(!topo.add_dependency('B', 'A', ()));

        let elements = ['A', 'B'];
        let result: Vec<_> = topo.sort_elements(&elements).collect();
        assert_eq!(result, vec!['B', 'A']);
    }

    /// The rejection is transitive, not just for immediate two-way pairs.
    #[test]
    fn longer_cycle_is_rejected() {
        let mut topo: TopoSort<i32, ()> = TopoSort::new();

        assert!(topo.add_dependency(1, 2, ()));
        assert!(topo.add_dependency(2, 3, ()));
        // 3 transitively depends on 1 already, via 2.
        assert!(!topo.add_dependency(3, 1, ()));

        let elements = [1, 2, 3];
        let result: Vec<_> = topo.sort_elements(&elements).collect();
        assert_eq!(result, vec![3, 2, 1]);
    }

    /// A self-edge is a cycle of length one.
    #[test]
    fn self_dependency_is_rejected() {
        let mut topo: TopoSort<i32, ()> = TopoSort::new();
        assert!(!topo.add_dependency(7, 7, ()));
        assert_eq!(
            topo.sort_elements(&[7]).collect::<Vec<_>>(),
            vec![7],
            "a rejected self-edge must not make the element unordered",
        );
    }

    /// Re-adding an existing edge overwrites its value and stays accepted, so the common
    /// "many points moving between the same pair of segments" path does no reachability walk.
    #[test]
    fn existing_edge_is_overwritten_not_rejected() {
        let mut topo: TopoSort<i32, i32> = TopoSort::new();
        assert!(topo.add_dependency(1, 2, 10));
        assert!(topo.add_dependency(1, 2, 20));
        assert_eq!(
            topo.dependencies_of(&1)
                .map(|(_, v)| *v)
                .collect::<Vec<_>>(),
            vec![20]
        );
        assert_eq!(topo.dependencies_of(&1).count(), 1, "no duplicate edge");
    }

    /// The sort reader still tolerates a cyclic graph if one is ever forced in, so a caller can
    /// never be handed a half-ordered list as if it were complete. `add_dependency` cannot build
    /// one, so the field is written directly here to keep the reader covered.
    #[test]
    fn circular_dependency() {
        // A B C D
        // A <- B <- C
        //   <- C <- D
        //   <- D <- B
        let elements = ['A', 'B', 'C', 'D'];

        let mut topo: TopoSort<char, ()> = TopoSort::new();
        topo.add_dependency('B', 'A', ());
        topo.add_dependency('C', 'A', ());
        topo.add_dependency('D', 'A', ());

        topo.add_dependency('C', 'B', ());
        topo.add_dependency('D', 'C', ());
        // Force the cycle in past `add_dependency`, which would reject it.
        topo.dependencies.entry('B').or_default().insert('D', ());

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
        topo.add_dependency(3, 4, ()); // This one is extra and should be ignored
        topo.add_dependency(2, 1, ());
        topo.add_dependency(3, 2, ());
        let result: Vec<_> = topo.sort_elements(&elements).collect();
        // 4 is missing, only element 2 is depending on 3
        assert_eq!(result, vec![1, 2, 3]);
    }
}
