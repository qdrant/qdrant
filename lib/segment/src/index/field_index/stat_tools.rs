use std::f64::consts::{E, PI};

/// This function estimates how many real points were selected with the filter.
/// It is assumed that each real point has, on average, X values in correspondence.
/// As a response to the execution of the query it is possible to establish only the number of matched associated values.
///
///
/// # Arguments
///
/// * `total_points` - total number of the unique points in the whole collection
/// * `total_values` - total number of payload values in the collection
/// * `selected_values_count` - amount of values selected during the query
///
/// # Result
///
/// Expected amount of unique points contained in selected values
/// The result might overflow at some corner cases
///   so it is better to limit its value with min and max
///
pub fn estimate_multi_value_selection_cardinality(
    total_points: usize,
    total_values: usize,
    selected_values_count: usize,
) -> f64 {
    // Value >= 1.0
    assert!(total_values >= total_points);
    let values_per_point = total_values as f64 / total_points as f64;
    // Probability to select each unique value
    let prob_select = 1. - prob_not_select(total_values, values_per_point, selected_values_count);
    prob_select * total_points as f64
}

/// Fast approximate computation of $ln(n!)$
/// See: <https://en.wikipedia.org/wiki/Stirling%27s_approximation>
fn approx_fact_log(n: f64) -> f64 {
    if n < 1.0 {
        return 1.0; // By definition
    }
    (2. * PI * n).sqrt().ln() + n * (n / E).ln()
}

/// Probability of each individual unique point to be selected with the query
///
/// Straight equation:
///     $\prod_{i=0}^{N-1} \frac{total - avg - i}{total - i}$
/// , where `N` - number of selected points
///
/// Proof:
///
/// $$
/// \prod_{i=0}^{N-1} \frac{total - avg - i}{total - i}
///     = \frac{\prod_{i=0}^{N-1} (total - avg - i)}{\prod_{i=0}^{N-1}(total - i)}
///     = \frac{\prod_{i=1}^{N} (total - avg - i + 1)}{\prod_{i=1}^{N}(total - i + 1)}\\
///     = \frac{\prod_{i=1}^{N} (total - avg - (N - i + 1) + 1)}{\prod_{i=1}^{N}(total - (N - i + 1) + 1)}
///     = \frac{\prod_{i=1}^{N} (i + total - avg - N)}{\prod_{i=1}^{N}(i + total - N)}\\
///     = \frac{\prod_{i=1}^{total - avg} i}{\prod_{i=1}^{total - avg - N} i} \frac{\prod_{i=1}^{total - N} i}{\prod_{i=1}^{total} i}
///     = \frac{(total - avg)!(total - N)!}{(total - avg - N)!(total)!}
///     = \exp(\ln{\frac{(total - avg)!(total - N)!}{(total - avg - N)!(total)!}})\\
///     = \exp(\ln((total - avg)!(total - N)!) - \ln((total - avg - N)!(total)!))
///     = \exp( \ln((total - avg)!) + \ln((total - N)!) - \ln((total - avg - N)!) - \ln(total!))
/// $$
///
/// Hint: use <https://latex.codecogs.com/eqneditor/editor.php> to render formula
fn prob_not_select(total: usize, avg: f64, selected: usize) -> f64 {
    let total = total as f64;
    let selected = selected as f64;
    (approx_fact_log(total - avg) + approx_fact_log(total - selected)
        - approx_fact_log(total - avg - selected)
        - approx_fact_log(total))
    .exp()
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use rand::prelude::StdRng;
    use rand::seq::SliceRandom;
    use rand::SeedableRng;

    use super::*;

    fn simulate(uniq: usize, avg: usize, selected: usize) -> usize {
        let mut data: Vec<_> = vec![];
        for i in 0..uniq {
            for _ in 0..avg {
                data.push(i);
            }
        }
        data.shuffle(&mut StdRng::seed_from_u64(42));

        let mut unique_selected: HashSet<_> = Default::default();
        for x in data.into_iter().take(selected) {
            unique_selected.insert(x);
        }

        unique_selected.len()
    }

    #[test]
    fn approx_factorial() {
        let approx = approx_fact_log(10.).exp();
        let real = (2 * 3 * 4 * 5 * 6 * 7 * 8 * 9 * 10) as f64;
        let error = (approx / real - 1.0).abs();
        assert!(error < 0.01);
    }

    #[test]
    fn test_estimation_corner_cases() {
        let count = estimate_multi_value_selection_cardinality(10, 20, 20);
        assert!(!count.is_nan());
        eprintln!("count = {:#?}", count);
        let count = estimate_multi_value_selection_cardinality(100, 100, 100);
        assert!(!count.is_nan());
        eprintln!("count = {:#?}", count);
        let count = estimate_multi_value_selection_cardinality(100, 100, 50);
        assert!(!count.is_nan());
        eprintln!("count = {:#?}", count);
        let count = estimate_multi_value_selection_cardinality(10, 10, 10);
        assert!(!count.is_nan());
        eprintln!("count = {:#?}", count);
        let count = estimate_multi_value_selection_cardinality(1, 1, 1);
        assert!(!count.is_nan());
        eprintln!("count = {:#?}", count);
        let count = estimate_multi_value_selection_cardinality(1, 1, 0);
        assert!(!count.is_nan());
        eprintln!("count = {:#?}", count);
    }

    #[test]
    fn test_estimation_1() {
        let total = 2000;
        let unique = 1000;
        let selected = 50;

        let estimation = estimate_multi_value_selection_cardinality(unique, total, selected);
        let experiment = simulate(unique, total / unique, selected);

        let error = (estimation / experiment as f64 - 1.0).abs();
        assert!(error < 0.05);
    }

    #[test]
    fn test_estimation_2() {
        let total = 2000;
        let unique = 1000;
        let selected = 300;

        let estimation = estimate_multi_value_selection_cardinality(unique, total, selected);
        let experiment = simulate(unique, total / unique, selected);
        let error = (estimation / experiment as f64 - 1.0).abs();
        assert!(error < 0.05);
    }
}
