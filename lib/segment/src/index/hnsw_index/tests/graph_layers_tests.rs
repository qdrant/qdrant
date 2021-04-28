use itertools::Itertools;

fn rev_range(a: usize, b: usize) -> impl Iterator<Item=usize> {
    (b + 1..=a).rev()
}

#[test]
fn test_graph_layer() {

    let r = rev_range(5, 0).collect_vec();

    eprintln!("r = {:#?}", r);
}