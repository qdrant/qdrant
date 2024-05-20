use segment::types::Score;

pub mod scored_point_ties;

pub fn assert_scores(score1: Option<Score>, score2: Option<Score>) {
    let (Some(Score::Float(score1)), Some(Score::Float(score2))) = (score1, score2) else {
        panic!("got unexpected Scores {score1:?} and {score2:?}");
    };
    assert!((score1 - score2).abs() < 0.0001)
}

pub fn assert_score(score: Option<Score>, expected: f64) {
    assert_eq!(score, Some(Score::Float(expected)));
}
