use std::collections::{BTreeMap, BTreeSet};
use std::hash::Hash;

use rand::RngExt;
use rand::rngs::StdRng;

pub fn gen_map<T: Eq + Ord + Hash>(
    rng: &mut StdRng,
    gen_key: impl Fn(&mut StdRng) -> T,
    count: usize,
) -> BTreeMap<T, BTreeSet<u32>> {
    let mut map = BTreeMap::new();

    for _ in 0..count {
        let key = repeat_until(|| gen_key(rng), |key| !map.contains_key(key));
        let set = (0..rng.random_range(1..=100))
            .map(|_| rng.random_range(0..=1000))
            .collect::<BTreeSet<_>>();
        map.insert(key, set);
    }

    map
}

pub fn gen_ident(rng: &mut StdRng) -> String {
    (0..rng.random_range(5..=32))
        .map(|_| rng.random_range(b'a'..=b'z') as char)
        .collect()
}

pub fn repeat_until<T>(mut f: impl FnMut() -> T, cond: impl Fn(&T) -> bool) -> T {
    std::iter::from_fn(|| Some(f())).find(|v| cond(v)).unwrap()
}
