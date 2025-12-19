use prometheus::proto::{Counter, Gauge, LabelPair, Metric, MetricFamily, MetricType};

pub(super) fn metric_family(
    name: &str,
    help: &str,
    r#type: MetricType,
    metrics: Vec<Metric>,
    prefix: Option<&str>,
) -> Option<MetricFamily> {
    // We can't create a new `MetricsFamily` without metrics.
    if metrics.is_empty() {
        return None;
    }

    let mut metric_family = MetricFamily::default();

    let name_with_prefix = prefix
        .map(|prefix| format!("{prefix}{name}"))
        .unwrap_or_else(|| name.to_string());

    metric_family.set_name(name_with_prefix);
    metric_family.set_help(help.into());
    metric_family.set_field_type(r#type);
    metric_family.set_metric(metrics);

    Some(metric_family)
}

pub(super) fn counter(value: f64, labels: &[(&str, &str)]) -> Metric {
    let mut metric = Metric::default();
    metric.set_label(labels.iter().map(|(n, v)| label_pair(n, v)).collect());
    metric.set_counter({
        let mut counter = Counter::default();
        counter.set_value(value);
        counter
    });
    metric
}

pub(super) fn gauge(value: f64, labels: &[(&str, &str)]) -> Metric {
    let mut metric = Metric::default();
    metric.set_label(labels.iter().map(|(n, v)| label_pair(n, v)).collect());
    metric.set_gauge({
        let mut gauge = Gauge::default();
        gauge.set_value(value);
        gauge
    });
    metric
}

pub(super) fn histogram(
    sample_count: u64,
    sample_sum: f64,
    buckets: &[(f64, u64)],
    labels: &[(&str, &str)],
) -> Metric {
    let mut metric = Metric::default();
    metric.set_label(labels.iter().map(|(n, v)| label_pair(n, v)).collect());
    metric.set_histogram({
        let mut histogram = prometheus::proto::Histogram::default();
        histogram.set_sample_count(sample_count);
        histogram.set_sample_sum(sample_sum);
        histogram.set_bucket(
            buckets
                .iter()
                .map(|&(upper_bound, cumulative_count)| {
                    let mut bucket = prometheus::proto::Bucket::default();
                    bucket.set_cumulative_count(cumulative_count);
                    bucket.set_upper_bound(upper_bound);
                    bucket
                })
                .collect(),
        );
        histogram
    });
    metric
}

fn label_pair(name: &str, value: &str) -> LabelPair {
    let mut label = LabelPair::default();
    label.set_name(name.into());
    label.set_value(value.into());
    label
}
