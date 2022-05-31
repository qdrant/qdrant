use itertools::Itertools;
use std::cell::Cell;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::collections::Bound::{Excluded, Included, Unbounded};
use std::ops::Bound;

#[derive(Debug, Clone)]
struct Node {
    center: Point,
    left_count: Cell<usize>,
    right_count: Cell<usize>,
}

impl Node {
    pub fn new(center: Point) -> Self {
        Node {
            center,
            left_count: Cell::new(0),
            right_count: Cell::new(0),
        }
    }
}

#[derive(PartialEq, PartialOrd, Debug, Clone)]
pub struct Point {
    pub val: f64,
    pub idx: usize,
}

impl Eq for Point {}

#[allow(clippy::derive_ord_xor_partial_ord)]
impl Ord for Point {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl Eq for Node {}

impl PartialEq<Self> for Node {
    fn eq(&self, other: &Self) -> bool {
        self.center.idx.eq(&other.center.idx)
    }
}

impl PartialOrd<Self> for Node {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.center.partial_cmp(&other.center)
    }
}

impl Ord for Node {
    fn cmp(&self, other: &Self) -> Ordering {
        self.center.partial_cmp(&other.center).unwrap()
    }
}

#[derive(Debug)]
pub struct Histogram {
    bucket_size: usize,
    borders: BTreeSet<Node>,
}

impl Histogram {
    pub fn new(bucket_size: usize) -> Self {
        Self {
            bucket_size,
            borders: BTreeSet::default(),
        }
    }

    pub fn estimate(&self, from: Bound<f64>, to: Bound<f64>) -> (usize, usize, usize) {
        let from_ = match &from {
            Included(val) => Included(Node::new(Point {
                val: *val,
                idx: usize::MIN,
            })),
            Excluded(val) => Excluded(Node::new(Point {
                val: *val,
                idx: usize::MAX,
            })),
            Unbounded => Unbounded,
        };

        let to_ = match &to {
            Included(val) => Included(Node::new(Point {
                val: *val,
                idx: usize::MAX,
            })),
            Excluded(val) => Excluded(Node::new(Point {
                val: *val,
                idx: usize::MIN,
            })),
            Unbounded => Unbounded,
        };

        // Value for range fraction estimation
        let from_val = match from {
            Included(val) => val,
            Excluded(val) => val,
            Unbounded => f64::MIN,
        };

        let to_val = match to {
            Included(val) => val,
            Excluded(val) => val,
            Unbounded => f64::MAX,
        };

        if from_val > to_val {
            return (0, 0, 0);
        }

        let left_border = {
            if matches!(from_, Unbounded) {
                None
            } else {
                self.borders.range((Unbounded, from_.clone())).next_back()
            }
        };

        let right_border = {
            if matches!(from_, Unbounded) {
                None
            } else {
                self.borders.range((to_.clone(), Unbounded)).next()
            }
        };

        let estimation = left_border
            .into_iter()
            .chain(self.borders.range((from_, to_)))
            .chain(right_border.into_iter())
            .tuple_windows()
            .map(|(a, b): (&Node, &Node)| {
                let val_range = b.center.val - a.center.val;

                if val_range == 0. {
                    // Zero-length range is always covered
                    let estimates = a.right_count.get() + 1;
                    return (estimates, estimates, estimates);
                }

                if a.right_count.get() == 0 {
                    // Range covers most-right border
                    return (1, 1, 1);
                }

                let cover_range = to_val.min(b.center.val) - from_val.max(a.center.val);

                let covered_frac = cover_range / val_range;
                let estimate = (a.right_count.get() as f64 * covered_frac).round() as usize + 1;

                let min_estimate = if cover_range == val_range {
                    a.right_count.get() + 1
                } else {
                    0
                };
                let max_estimate = a.right_count.get() + 1;
                (min_estimate, estimate, max_estimate)
            })
            .reduce(|a, b| (a.0 + b.0, a.1 + b.1, a.2 + b.2))
            .unwrap_or((0, 0, 0));

        estimation
    }

    pub fn insert<F, G>(&mut self, val: Point, left_neighbour: F, right_neighbour: G)
    where
        F: Fn(&Point) -> Option<Point>,
        G: Fn(&Point) -> Option<Point>,
    {
        if self.borders.len() < 2 {
            self.borders.insert(Node {
                center: val,
                left_count: Cell::new(0),
                right_count: Cell::new(0),
            });
            return;
        }

        let (close_neighbors, (far_left_neighbor, far_right_neighbor)) = {
            let mut left_iterator = self
                .borders
                .range((Unbounded, Excluded(Node::new(val.clone()))));
            let mut right_iterator = self
                .borders
                .range((Excluded(Node::new(val.clone())), Unbounded));
            (
                (left_iterator.next_back(), right_iterator.next()),
                (left_iterator.next_back(), right_iterator.next()),
            )
        };

        let (to_remove, to_create) = match close_neighbors {
            (None, Some(right_border)) => {
                // x|.....|...
                let new_count = right_border.right_count.get() + 1;
                let new_border = Node {
                    center: val,
                    left_count: Cell::new(0),
                    right_count: Cell::new(new_count),
                };

                if new_count > self.bucket_size {
                    // Too many values, can't move the border
                    // x|.....|...
                    // ||.....|...
                    new_border.right_count.set(0);
                    (None, Some(new_border))
                } else {
                    // x|.....|...
                    // |......|...
                    far_right_neighbor.unwrap().left_count.set(new_count);
                    (Some(right_border.clone()), Some(new_border))
                }
            }
            (Some(left_border), None) => {
                // ...|.....|x
                let new_count = left_border.left_count.get() + 1;
                let new_border = Node {
                    center: val,
                    left_count: Cell::new(new_count),
                    right_count: Cell::new(0),
                };
                if new_count > self.bucket_size {
                    // Too many values, can't move the border
                    // ...|.....|x
                    // ...|.....||
                    new_border.left_count.set(0);
                    (None, Some(new_border))
                } else {
                    // ...|.....|x
                    // ...|......|
                    far_left_neighbor.unwrap().right_count.set(new_count);
                    (Some(left_border.clone()), Some(new_border))
                }
            }
            (Some(left_border), Some(right_border)) => {
                if left_border.right_count.get() != right_border.left_count.get() {
                    eprintln!("error");
                }
                assert_eq!(left_border.right_count.get(), right_border.left_count.get());
                let new_count = left_border.right_count.get() + 1;

                if new_count > self.bucket_size {
                    // Too many values, let's adjust
                    // Decide which border to move

                    let left_dist = (val.val - left_border.center.val).abs();
                    let right_dist = (val.val - right_border.center.val).abs();
                    if left_dist < right_dist {
                        // left border closer:
                        //  ...|..x.........|...
                        let new_border = Node {
                            center: right_neighbour(&left_border.center).unwrap(),
                            left_count: Cell::new(left_border.left_count.get() + 1),
                            right_count: Cell::new(left_border.right_count.get()),
                        };

                        if left_border.left_count.get() < self.bucket_size
                            && far_left_neighbor.is_some()
                        {
                            //we can move
                            //  ...|..x.........|...
                            //  ....|.x.........|...
                            if let Some(x) = far_left_neighbor {
                                x.right_count.set(new_border.left_count.get())
                            };
                            (Some(left_border.clone()), Some(new_border))
                        } else {
                            // Can't be moved anymore, create an additional one
                            //  ...|..x.........|...
                            //  ...||.x.........|...
                            new_border.left_count.set(0);
                            left_border.right_count.set(0);
                            (None, Some(new_border))
                        }
                    } else {
                        // right border closer
                        //  ...|........x...|...
                        let new_border = Node {
                            center: left_neighbour(&right_border.center).unwrap(),
                            left_count: Cell::new(right_border.left_count.get()),
                            right_count: Cell::new(right_border.right_count.get() + 1),
                        };

                        if right_border.right_count.get() < self.bucket_size
                            && far_right_neighbor.is_some()
                        {
                            // it's ok, we can move
                            //  1: ...|........x...|...
                            //  2: ...|........x..|....
                            if let Some(x) = far_right_neighbor {
                                x.left_count.set(new_border.right_count.get())
                            }
                            (Some(right_border.clone()), Some(new_border))
                        } else {
                            // Can't be moved anymore, create a new one
                            //  1: ...|........x...|...
                            //  2: ...|........x..||...
                            new_border.right_count.set(0);
                            right_border.left_count.set(0);
                            (None, Some(new_border))
                        }
                    }
                } else {
                    left_border.right_count.set(new_count);
                    right_border.left_count.set(new_count);
                    (None, None)
                }
            }
            (None, None) => unreachable!(),
        };

        if let Some(remove_border) = to_remove {
            self.borders.remove(&remove_border);
        }

        if let Some(new_border) = to_create {
            self.borders.insert(new_border);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::Itertools;
    use rand::prelude::StdRng;
    use rand::{Rng, SeedableRng};
    use rand_distr::StandardNormal;

    #[allow(dead_code)]
    fn print_results(points_index: &BTreeSet<Point>, histogram: &Histogram, pnt: Option<Point>) {
        for point in points_index.iter() {
            if let Some(border) = histogram.borders.get(&Node {
                center: point.clone(),
                left_count: Cell::new(0),
                right_count: Cell::new(0),
            }) {
                print!(" {}|{} ", border.left_count.get(), border.right_count.get());
            } else if pnt.is_some() && pnt.as_ref().unwrap().idx == point.idx {
                print!("x");
            } else {
                print!(".");
            }
        }
        println!();
    }

    fn count_range(points_index: &BTreeSet<Point>, a: f64, b: f64) -> usize {
        points_index
            .iter()
            .filter(|x| a <= x.val && x.val <= b)
            .count()
    }

    #[test]
    fn test_build_histogram_small() {
        let bucket_size = 5;
        let num_samples = 1000;
        let mut rnd = StdRng::seed_from_u64(42);

        // let points = (0..100000).map(|i| Point { val: rnd.gen_range(-10.0..10.0), idx: i }).collect_vec();
        let points = (0..num_samples)
            .map(|i| Point {
                val: f64::round(rnd.sample::<f64, _>(StandardNormal) * 10.0),
                idx: i,
            })
            .collect_vec();

        let mut points_index: BTreeSet<Point> = Default::default();

        let mut histogram = Histogram::new(bucket_size);

        for point in points {
            points_index.insert(point.clone());
            // print_results(&points_index, &histogram, Some(point.clone()));
            histogram.insert(
                point,
                |x| {
                    points_index
                        .range((Unbounded, Excluded(x)))
                        .next_back()
                        .cloned()
                },
                |x| points_index.range((Excluded(x), Unbounded)).next().cloned(),
            );
        }
    }

    #[test]
    fn test_build_histogram_round() {
        let bucket_size = 10;
        let num_samples = 100_000;
        let mut rnd = StdRng::seed_from_u64(42);

        // let points = (0..100000).map(|i| Point { val: rnd.gen_range(-10.0..10.0), idx: i }).collect_vec();
        let points = (0..num_samples).map(|i| Point {
            val: f64::round(rnd.sample::<f64, _>(StandardNormal) * 100.0),
            idx: i,
        });

        let mut points_index: BTreeSet<Point> = Default::default();

        let mut histogram = Histogram::new(bucket_size);

        let read_counter = Cell::new(0);
        for point in points {
            points_index.insert(point.clone());
            // print_results(&points_index, &histogram, Some(point.clone()));
            histogram.insert(
                point,
                |x| {
                    read_counter.set(read_counter.get() + 1);
                    points_index
                        .range((Unbounded, Excluded(x)))
                        .next_back()
                        .cloned()
                },
                |x| {
                    read_counter.set(read_counter.get() + 1);
                    points_index.range((Excluded(x), Unbounded)).next().cloned()
                },
            );
        }
        eprintln!("read_counter.get() = {:#?}", read_counter.get());
        eprintln!("histogram.borders.len() = {:#?}", histogram.borders.len());
        for border in histogram.borders.iter().take(2) {
            eprintln!("border = {:#?}", border);
        }

        let (est_min, estimation, est_max) = histogram.estimate(Included(0.0), Included(0.0));
        let real = count_range(&points_index, 0., 0.);

        eprintln!(
            "{} / ({}, {}, {}) = {}",
            real,
            est_min,
            estimation,
            est_max,
            estimation as f64 / real as f64
        );
        assert!(real.abs_diff(estimation) < 2 * bucket_size);

        let (est_min, estimation, est_max) = histogram.estimate(Included(0.0), Included(0.0001));
        let real = count_range(&points_index, 0., 0.0001);

        eprintln!(
            "{} / ({}, {}, {}) = {}",
            real,
            est_min,
            estimation,
            est_max,
            estimation as f64 / real as f64
        );
        assert!(real.abs_diff(estimation) < 2 * bucket_size);

        let (est_min, estimation, est_max) = histogram.estimate(Included(0.0), Included(0.01));
        let real = count_range(&points_index, 0., 0.01);

        eprintln!(
            "{} / ({}, {}, {}) = {}",
            real,
            est_min,
            estimation,
            est_max,
            estimation as f64 / real as f64
        );
        assert!(real.abs_diff(estimation) < 2 * bucket_size);

        let (est_min, estimation, est_max) = histogram.estimate(Included(0.), Included(1.));
        let real = count_range(&points_index, 0., 1.);

        eprintln!(
            "{} / ({}, {}, {}) = {}",
            real,
            est_min,
            estimation,
            est_max,
            estimation as f64 / real as f64
        );
        assert!(real.abs_diff(estimation) < 2 * bucket_size);

        let (est_min, estimation, est_max) = histogram.estimate(Included(0.), Included(100.));
        let real = count_range(&points_index, 0., 100.);

        eprintln!(
            "{} / ({}, {}, {}) = {}",
            real,
            est_min,
            estimation,
            est_max,
            estimation as f64 / real as f64
        );
        assert!(real.abs_diff(estimation) < 2 * bucket_size);

        let (est_min, estimation, est_max) = histogram.estimate(Included(-100.), Included(100.));
        let real = count_range(&points_index, -100., 100.);

        eprintln!(
            "{} / ({}, {}, {}) = {}",
            real,
            est_min,
            estimation,
            est_max,
            estimation as f64 / real as f64
        );
        assert!(real.abs_diff(estimation) < 2 * bucket_size);

        let (est_min, estimation, est_max) = histogram.estimate(Included(20.), Included(100.));
        let real = count_range(&points_index, 20., 100.);

        eprintln!(
            "{} / ({}, {}, {}) = {}",
            real,
            est_min,
            estimation,
            est_max,
            estimation as f64 / real as f64
        );
        assert!(real.abs_diff(estimation) < 2 * bucket_size);
    }

    #[test]
    fn test_build_histogram() {
        let bucket_size = 100;
        let num_samples = 100_000;
        let mut rnd = StdRng::seed_from_u64(42);

        // let points = (0..100000).map(|i| Point { val: rnd.gen_range(-10.0..10.0), idx: i }).collect_vec();
        let points = (0..num_samples)
            .map(|i| Point {
                val: rnd.sample(StandardNormal),
                idx: i,
            })
            .collect_vec();

        let mut points_index: BTreeSet<Point> = Default::default();

        let mut histogram = Histogram::new(bucket_size);

        let read_counter = Cell::new(0);
        for point in points {
            points_index.insert(point.clone());
            // print_results(&points_index, &histogram, Some(point.clone()));
            histogram.insert(
                point,
                |x| {
                    read_counter.set(read_counter.get() + 1);
                    points_index
                        .range((Unbounded, Excluded(x)))
                        .next_back()
                        .cloned()
                },
                |x| {
                    read_counter.set(read_counter.get() + 1);
                    points_index.range((Excluded(x), Unbounded)).next().cloned()
                },
            );
        }
        eprintln!("read_counter.get() = {:#?}", read_counter.get());
        eprintln!("histogram.borders.len() = {:#?}", histogram.borders.len());
        for border in histogram.borders.iter().take(2) {
            eprintln!("border = {:#?}", border);
        }

        let (est_min, estimation, est_max) = histogram.estimate(Included(0.0), Included(0.0));
        let real = count_range(&points_index, 0., 0.);

        eprintln!(
            "{} / ({}, {}, {}) = {}",
            real,
            est_min,
            estimation,
            est_max,
            estimation as f64 / real as f64
        );
        assert!(real.abs_diff(estimation) < bucket_size);

        let (est_min, estimation, est_max) = histogram.estimate(Included(0.0), Included(0.0001));
        let real = count_range(&points_index, 0., 0.0001);

        eprintln!(
            "{} / ({}, {}, {}) = {}",
            real,
            est_min,
            estimation,
            est_max,
            estimation as f64 / real as f64
        );
        assert!(real.abs_diff(estimation) < bucket_size);

        let (est_min, estimation, est_max) = histogram.estimate(Included(0.0), Included(0.01));
        let real = count_range(&points_index, 0., 0.01);

        eprintln!(
            "{} / ({}, {}, {}) = {}",
            real,
            est_min,
            estimation,
            est_max,
            estimation as f64 / real as f64
        );
        assert!(real.abs_diff(estimation) < bucket_size);

        let (est_min, estimation, est_max) = histogram.estimate(Included(0.), Included(1.));
        let real = count_range(&points_index, 0., 1.);

        eprintln!(
            "{} / ({}, {}, {}) = {}",
            real,
            est_min,
            estimation,
            est_max,
            estimation as f64 / real as f64
        );
        assert!(real.abs_diff(estimation) < bucket_size);

        let (est_min, estimation, est_max) = histogram.estimate(Included(0.), Included(100.));
        let real = count_range(&points_index, 0., 100.);

        eprintln!(
            "{} / ({}, {}, {}) = {}",
            real,
            est_min,
            estimation,
            est_max,
            estimation as f64 / real as f64
        );
        assert!(real.abs_diff(estimation) < bucket_size);

        let (est_min, estimation, est_max) = histogram.estimate(Included(-100.), Included(100.));
        let real = count_range(&points_index, -100., 100.);

        eprintln!(
            "{} / ({}, {}, {}) = {}",
            real,
            est_min,
            estimation,
            est_max,
            estimation as f64 / real as f64
        );
        assert!(real.abs_diff(estimation) < bucket_size);

        let (est_min, estimation, est_max) = histogram.estimate(Included(20.), Included(100.));
        let real = count_range(&points_index, 20., 100.);

        eprintln!(
            "{} / ({}, {}, {}) = {}",
            real,
            est_min,
            estimation,
            est_max,
            estimation as f64 / real as f64
        );
        assert!(real.abs_diff(estimation) < bucket_size);
    }
}
