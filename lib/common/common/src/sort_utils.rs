use std::collections::HashMap;
use std::hash::Hash;

pub fn sort_permutation<T: Eq + Hash + Copy, V>(
    values: &mut [V],
    keys: &[T],
    sort: impl FnOnce(&[T]) -> Vec<T>,
) {
    if values.len() <= 1 {
        return;
    }

    let sorted_keys = sort(keys);

    // Build a map from key to its target position in the sorted order
    let key_to_target_idx: HashMap<T, usize> = sorted_keys
        .into_iter()
        .enumerate()
        .map(|(idx, key)| (key, idx))
        .collect();

    // Build permutation: perm[i] = where element at index i should go
    let mut perm: Vec<usize> = keys.iter().map(|k| key_to_target_idx[k]).collect();

    // Apply permutation in-place using cycle sort

    // Example:
    // keys: [K1, k3, K0, K2]
    // sorted_keys: [K0, K1, K2, K3]
    // perm: [1, 3, 0, 2]
    for i in 0..perm.len() {
        // Example step 1:
        // i = 0, perm[0] = 1
        // swap perm[0] and perm[1] -> [K3, K1, K0, K2]
        // i = 0, perm[0] = 3
        // swap perm[0] and perm[3] -> [K2, K1, K0, K3]
        // i = 0, perm[0] = 2
        // swap perm[0] and perm[2] -> [K0, K1, K2, K3]
        while perm[i] != i {
            let target = perm[i];
            values.swap(i, target);
            perm.swap(i, target);
        }
    }
}
