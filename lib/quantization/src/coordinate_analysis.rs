use std::fmt::Write as FmtWrite;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use crate::encoded_vectors::VectorParameters;

const NUM_BINS: usize = 50;
const CHART_W: usize = 400;
const CHART_H: usize = 300;
const MARGIN_L: usize = 60;
const MARGIN_R: usize = 20;
const MARGIN_T: usize = 35;
const MARGIN_B: usize = 40;

const OUTPUT_DIR: &str = "/Users/pleshkov/qdrant/target/analysis";

/// Create a new timestamped run directory under the analysis output folder.
/// Returns the path like `target/analysis/result_2026-04-01_14-30-05/`.
pub fn create_run_dir() -> PathBuf {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    let secs = now.as_secs();

    // Convert to a human-readable UTC timestamp without pulling in chrono.
    let (y, mo, d, h, mi, s) = epoch_to_utc(secs);
    let name = format!("result_{y:04}-{mo:02}-{d:02}_{h:02}-{mi:02}-{s:02}");

    let dir = Path::new(OUTPUT_DIR).join(name);
    fs_err::create_dir_all(&dir).expect("Failed to create analysis run directory");
    dir
}

fn epoch_to_utc(secs: u64) -> (u64, u64, u64, u64, u64, u64) {
    let s = secs % 60;
    let mi = (secs / 60) % 60;
    let h = (secs / 3600) % 24;
    let mut days = secs / 86400;

    let mut y = 1970u64;
    loop {
        let days_in_year = if is_leap(y) { 366 } else { 365 };
        if days < days_in_year {
            break;
        }
        days -= days_in_year;
        y += 1;
    }

    let month_days: [u64; 12] = if is_leap(y) {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };
    let mut mo = 1u64;
    for &md in &month_days {
        if days < md {
            break;
        }
        days -= md;
        mo += 1;
    }
    let d = days + 1;
    (y, mo, d, h, mi, s)
}

fn is_leap(y: u64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

struct Histogram {
    bins: Vec<usize>,
    min_val: f32,
    max_val: f32,
    mean_val: f32,
    median_val: f32,
    ks_statistic: f32,
    coord_idx: usize,
    distortion: f32,
}

/// Approximate the normal CDF using the Abramowitz & Stegun rational approximation.
fn normal_cdf(x: f64) -> f64 {
    const A1: f64 = 0.254829592;
    const A2: f64 = -0.284496736;
    const A3: f64 = 1.421413741;
    const A4: f64 = -1.453152027;
    const A5: f64 = 1.061405429;
    const P: f64 = 0.3275911;

    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x = x.abs();
    let t = 1.0 / (1.0 + P * x);
    let y = 1.0 - (((((A5 * t + A4) * t) + A3) * t + A2) * t + A1) * t * (-x * x / 2.0).exp();
    0.5 * (1.0 + sign * y)
}

/// Kolmogorov-Smirnov statistic: max |F_empirical(x) - F_normal(x)|.
/// `sorted` must be pre-sorted in ascending order.
fn ks_normal(sorted: &[f32], mean: f64, std_dev: f64) -> f32 {
    if sorted.is_empty() || std_dev <= 0.0 {
        return 1.0;
    }
    let n = sorted.len() as f64;
    let mut max_d: f64 = 0.0;
    for (i, &v) in sorted.iter().enumerate() {
        let cdf = normal_cdf((v as f64 - mean) / std_dev);
        // empirical CDF jumps from i/n to (i+1)/n at sorted[i]
        let d1 = ((i + 1) as f64 / n - cdf).abs();
        let d2 = (i as f64 / n - cdf).abs();
        max_d = max_d.max(d1).max(d2);
    }
    max_d as f32
}

fn build_histogram(values: &[f32], coord_idx: usize) -> Histogram {
    if values.is_empty() {
        return Histogram {
            bins: vec![0; NUM_BINS],
            min_val: 0.0,
            max_val: 0.0,
            mean_val: 0.0,
            median_val: 0.0,
            ks_statistic: 1.0,
            coord_idx,
            distortion: 0.0,
        };
    }

    let n = values.len() as f32;
    let mean = values.iter().sum::<f32>() / n;
    let variance = values.iter().map(|&v| (v - mean) * (v - mean)).sum::<f32>() / n;
    let std_dev = variance.sqrt();

    let min_val = values.iter().copied().fold(f32::INFINITY, f32::min);
    let max_val = values.iter().copied().fold(f32::NEG_INFINITY, f32::max);

    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let median = if sorted.len() % 2 == 0 {
        (sorted[sorted.len() / 2 - 1] + sorted[sorted.len() / 2]) / 2.0
    } else {
        sorted[sorted.len() / 2]
    };

    let mut bins = vec![0usize; NUM_BINS];
    let range = max_val - min_val;
    if range > 0.0 {
        for &v in values {
            let idx = ((v - min_val) / range * (NUM_BINS - 1) as f32) as usize;
            bins[idx.min(NUM_BINS - 1)] += 1;
        }
    } else {
        bins[NUM_BINS / 2] = values.len();
    }

    let ks = ks_normal(&sorted, mean as f64, std_dev as f64);

    Histogram {
        bins,
        min_val,
        max_val,
        mean_val: mean,
        median_val: median,
        ks_statistic: ks,
        coord_idx,
        distortion: std_dev,
    }
}

fn render_histogram_inner(
    svg: &mut String,
    hist: &Histogram,
    title: &str,
    x_off: usize,
    y_off: usize,
    bar_color: &str,
) {
    let plot_w = CHART_W - MARGIN_L - MARGIN_R;
    let plot_h = CHART_H - MARGIN_T - MARGIN_B;
    let max_count = *hist.bins.iter().max().unwrap_or(&1).max(&1);
    let bar_w = plot_w as f64 / NUM_BINS as f64;

    let _ = writeln!(svg, "<g transform=\"translate({x_off},{y_off})\">");
    let _ = writeln!(
        svg,
        "<rect width=\"{CHART_W}\" height=\"{CHART_H}\" fill=\"white\" stroke=\"#ccc\"/>"
    );

    // Title
    let _ = writeln!(
        svg,
        "<text x=\"{}\" y=\"22\" text-anchor=\"middle\" font-size=\"12\" \
         font-family=\"sans-serif\" font-weight=\"bold\">{title}</text>",
        CHART_W / 2
    );

    // Bars
    for (i, &count) in hist.bins.iter().enumerate() {
        let bar_h = if max_count > 0 {
            (count as f64 / max_count as f64) * plot_h as f64
        } else {
            0.0
        };
        let x = MARGIN_L as f64 + i as f64 * bar_w;
        let y = MARGIN_T as f64 + plot_h as f64 - bar_h;
        let _ = writeln!(
            svg,
            "<rect x=\"{x:.1}\" y=\"{y:.1}\" width=\"{:.1}\" height=\"{bar_h:.1}\" fill=\"{bar_color}\"/>",
            (bar_w * 0.9).max(1.0)
        );
    }

    // Vertical line at zero
    let range = hist.max_val - hist.min_val;
    if range > 0.0 && hist.min_val <= 0.0 && hist.max_val >= 0.0 {
        let zero_frac = (0.0 - hist.min_val) / range;
        let zero_x = MARGIN_L as f64 + zero_frac as f64 * plot_w as f64;
        let _ = writeln!(
            svg,
            "<line x1=\"{zero_x:.1}\" y1=\"{MARGIN_T}\" x2=\"{zero_x:.1}\" y2=\"{}\" \
             stroke=\"black\" stroke-width=\"1\" stroke-dasharray=\"1,2\"/>",
            MARGIN_T + plot_h,
        );
    }

    // Vertical lines at mean and median
    if range > 0.0 {
        let mean_frac = (hist.mean_val - hist.min_val) / range;
        let mean_x = MARGIN_L as f64 + mean_frac as f64 * plot_w as f64;
        let _ = writeln!(
            svg,
            "<line x1=\"{mean_x:.1}\" y1=\"{MARGIN_T}\" x2=\"{mean_x:.1}\" y2=\"{}\" \
             stroke=\"red\" stroke-width=\"1.5\" stroke-dasharray=\"4,3\"/>",
            MARGIN_T + plot_h,
        );

        let median_frac = (hist.median_val - hist.min_val) / range;
        let median_x = MARGIN_L as f64 + median_frac as f64 * plot_w as f64;
        let _ = writeln!(
            svg,
            "<line x1=\"{median_x:.1}\" y1=\"{MARGIN_T}\" x2=\"{median_x:.1}\" y2=\"{}\" \
             stroke=\"#9c27b0\" stroke-width=\"1.5\" stroke-dasharray=\"2,2\"/>",
            MARGIN_T + plot_h,
        );
    }

    // Axes
    let _ = writeln!(
        svg,
        "<line x1=\"{MARGIN_L}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" stroke=\"black\"/>",
        MARGIN_T + plot_h,
        MARGIN_L + plot_w,
        MARGIN_T + plot_h,
    );
    let _ = writeln!(
        svg,
        "<line x1=\"{MARGIN_L}\" y1=\"{MARGIN_T}\" x2=\"{MARGIN_L}\" y2=\"{}\" stroke=\"black\"/>",
        MARGIN_T + plot_h,
    );

    // X-axis min/max labels
    let _ = writeln!(
        svg,
        "<text x=\"{MARGIN_L}\" y=\"{}\" font-size=\"10\" font-family=\"sans-serif\">{:.3}</text>",
        CHART_H - 5,
        hist.min_val,
    );
    let _ = writeln!(
        svg,
        "<text x=\"{}\" y=\"{}\" text-anchor=\"end\" font-size=\"10\" \
         font-family=\"sans-serif\">{:.3}</text>",
        MARGIN_L + plot_w,
        CHART_H - 5,
        hist.max_val,
    );

    // Y-axis max count label
    let _ = writeln!(
        svg,
        "<text x=\"5\" y=\"{}\" font-size=\"10\" font-family=\"sans-serif\">{max_count}</text>",
        MARGIN_T + 10,
    );

    // Mean, median, distortion, KS annotation
    let _ = writeln!(
        svg,
        "<text x=\"{}\" y=\"{}\" text-anchor=\"end\" font-size=\"10\" \
         font-family=\"sans-serif\">\
         <tspan fill=\"red\">μ={:.4}</tspan>  \
         <tspan fill=\"#9c27b0\">med={:.4}</tspan>  \
         <tspan fill=\"#666\">σ={:.4}</tspan>  \
         <tspan fill=\"#e65100\">KS={:.4}</tspan></text>",
        CHART_W - 5,
        MARGIN_T - 5,
        hist.mean_val,
        hist.median_val,
        hist.distortion,
        hist.ks_statistic,
    );

    let _ = writeln!(svg, "</g>");
}

fn render_single_histogram(hist: &Histogram, title: &str, bar_color: &str) -> String {
    let mut svg = String::new();
    let _ = writeln!(
        svg,
        "<svg width=\"{CHART_W}\" height=\"{CHART_H}\" xmlns=\"http://www.w3.org/2000/svg\">"
    );
    render_histogram_inner(&mut svg, hist, title, 0, 0, bar_color);
    let _ = writeln!(svg, "</svg>");
    svg
}

fn render_grid(histograms: &[Histogram], sorted_by_distortion: &[usize]) -> String {
    let cols = 4;
    let total_w = CHART_W * cols;
    let row_height = CHART_H + 25;
    let total_h = row_height * 4 + 20;
    let mut svg = String::new();
    let _ = writeln!(
        svg,
        "<svg width=\"{total_w}\" height=\"{total_h}\" xmlns=\"http://www.w3.org/2000/svg\">"
    );
    let _ = writeln!(svg, "<rect width=\"100%\" height=\"100%\" fill=\"#f8f8f8\"/>");

    let n = sorted_by_distortion.len();
    if n == 0 {
        let _ = writeln!(svg, "</svg>");
        return svg;
    }

    // Sort by KS statistic (ascending) for the 4th row
    let mut sorted_by_ks: Vec<usize> = (0..histograms.len()).collect();
    sorted_by_ks.sort_by(|&a, &b| {
        histograms[a]
            .ks_statistic
            .partial_cmp(&histograms[b].ks_statistic)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let row_labels = [
        "Min Distortion (σ)",
        "Avg Distortion (σ)",
        "Max Distortion (σ)",
        "Worst KS (least normal)",
    ];
    let row_colors = ["#4caf50", "steelblue", "#e53935", "#e65100"];

    let pick = |sorted: &[usize], center: usize| -> Vec<usize> {
        if n >= cols {
            let lo = center.saturating_sub(cols / 2).min(n - cols);
            (lo..lo + cols).map(|i| sorted[i]).collect()
        } else {
            sorted.to_vec()
        }
    };

    let rows = [
        pick(sorted_by_distortion, 0),                        // min distortion
        pick(sorted_by_distortion, n / 2),                    // avg distortion
        pick(sorted_by_distortion, n.saturating_sub(1)),       // max distortion
        pick(&sorted_by_ks, n.saturating_sub(1)),              // worst KS
    ];

    for (row_idx, row) in rows.iter().enumerate() {
        let y_base = row_idx * row_height + 20;
        let _ = writeln!(
            svg,
            "<text x=\"10\" y=\"{}\" font-size=\"14\" font-family=\"sans-serif\" \
             font-weight=\"bold\" fill=\"{}\">{}</text>",
            y_base,
            row_colors[row_idx],
            row_labels[row_idx],
        );
        for (col_idx, &coord_idx) in row.iter().enumerate() {
            let x_off = col_idx * CHART_W;
            let y_off = y_base + 5;
            let hist = &histograms[coord_idx];
            let title = format!(
                "Coord {} (σ={:.4} KS={:.4})",
                hist.coord_idx, hist.distortion, hist.ks_statistic
            );
            render_histogram_inner(&mut svg, hist, &title, x_off, y_off, row_colors[row_idx]);
        }
    }

    let _ = writeln!(svg, "</svg>");
    svg
}

pub fn analyse<'a>(
    data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone,
    vector_parameters: &VectorParameters,
    count: usize,
    run_dir: &Path,
    prefix: &str,
) {
    let dim = vector_parameters.dim;

    // Collect per-coordinate values
    let mut coord_values: Vec<Vec<f32>> = vec![Vec::with_capacity(count); dim];
    for vector in data {
        let v = vector.as_ref();
        for (i, &val) in v.iter().take(dim).enumerate() {
            coord_values[i].push(val);
        }
    }

    // Build histograms for every coordinate
    let histograms: Vec<Histogram> = coord_values
        .iter()
        .enumerate()
        .map(|(i, values)| build_histogram(values, i))
        .collect();

    // Create output directory: <run_dir>/<prefix>/
    let output_dir = run_dir.join(prefix);
    fs_err::create_dir_all(&output_dir).expect("Failed to create analysis output directory");

    // Save individual histograms into a coord/ subfolder
    let coord_dir = output_dir.join("coord");
    fs_err::create_dir_all(&coord_dir).expect("Failed to create coord output directory");
    for hist in &histograms {
        let svg = render_single_histogram(
            hist,
            &format!("Coordinate {}", hist.coord_idx),
            "steelblue",
        );
        let path = coord_dir.join(format!("coord_{}.svg", hist.coord_idx));
        fs_err::write(path, svg).expect("Failed to write histogram SVG");
    }

    // Sort coordinates by distortion (ascending)
    let mut sorted_indices: Vec<usize> = (0..histograms.len()).collect();
    sorted_indices.sort_by(|&a, &b| {
        histograms[a]
            .distortion
            .partial_cmp(&histograms[b].distortion)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    // 4×4 grid: min σ / avg σ / max σ / worst KS rows
    let grid_svg = render_grid(&histograms, &sorted_indices);
    fs_err::write(output_dir.join("grid_4x4.svg"), grid_svg).expect("Failed to write grid SVG");

    // Distributions: mean, median, distortion (σ), KS across all coordinates
    let means: Vec<f32> = histograms.iter().map(|h| h.mean_val).collect();
    let medians: Vec<f32> = histograms.iter().map(|h| h.median_val).collect();
    let distortions: Vec<f32> = histograms.iter().map(|h| h.distortion).collect();
    let ks_values: Vec<f32> = histograms.iter().map(|h| h.ks_statistic).collect();

    let mean_hist = build_histogram(&means, 0);
    let median_hist = build_histogram(&medians, 0);
    let dist_hist = build_histogram(&distortions, 0);
    let ks_hist = build_histogram(&ks_values, 0);

    let total_w = CHART_W;
    let total_h = CHART_H * 4;
    let mut svg = String::new();
    let _ = writeln!(
        svg,
        "<svg width=\"{total_w}\" height=\"{total_h}\" xmlns=\"http://www.w3.org/2000/svg\">"
    );
    let _ = writeln!(svg, "<rect width=\"100%\" height=\"100%\" fill=\"white\"/>");
    render_histogram_inner(&mut svg, &mean_hist, "Mean (μ) per coordinate", 0, 0, "red");
    render_histogram_inner(&mut svg, &median_hist, "Median per coordinate", 0, CHART_H, "#9c27b0");
    render_histogram_inner(&mut svg, &dist_hist, "Distortion (σ) per coordinate", 0, CHART_H * 2, "#ff9800");
    render_histogram_inner(&mut svg, &ks_hist, "KS statistic per coordinate", 0, CHART_H * 3, "#e65100");
    let _ = writeln!(svg, "</svg>");

    fs_err::write(output_dir.join("distributions.svg"), svg)
        .expect("Failed to write distributions SVG");

    eprintln!(
        "[coordinate_analysis] Saved {} histograms + grid + distributions to {}",
        histograms.len(),
        output_dir.display(),
    );
}
