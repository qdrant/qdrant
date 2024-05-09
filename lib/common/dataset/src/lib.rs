use std::fs::{create_dir_all, File};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use flate2::read::GzDecoder;
use indicatif::{ProgressBar, ProgressDrawTarget};

pub enum Dataset {
    // https://github.com/qdrant/sparse-vectors-experiments
    SpladeWikiMovies,

    // https://github.com/qdrant/sparse-vectors-benchmark
    NeurIps2023Full,
    NeurIps2023_1M,
    NeurIps2023Small,
    NeurIps2023Queries,
}

impl Dataset {
    pub fn download(&self) -> Result<PathBuf> {
        download_cached(&self.url())
    }

    fn url(&self) -> String {
        const NEUR_IPS_2023_BASE: &str =
            "https://storage.googleapis.com/ann-challenge-sparse-vectors/csr";
        match self {
            Dataset::SpladeWikiMovies => {
                "https://storage.googleapis.com/dataset-sparse-vectors/sparse-vectors.jsonl.gz"
                    .to_string()
            }
            Dataset::NeurIps2023Full => format!("{NEUR_IPS_2023_BASE}/base_full.csr.gz"),
            Dataset::NeurIps2023_1M => format!("{NEUR_IPS_2023_BASE}/base_1M.csr.gz"),
            Dataset::NeurIps2023Small => format!("{NEUR_IPS_2023_BASE}/base_small.csr.gz"),
            Dataset::NeurIps2023Queries => format!("{NEUR_IPS_2023_BASE}/queries.dev.csr.gz"),
        }
    }
}

fn download_cached(url: &str) -> Result<PathBuf> {
    // Filename without an ".gz" extension, e.g. "base_full.csr".
    let basename = {
        let path = Path::new(url);
        match path.extension() {
            Some(gz) if gz == "gz" => path.file_stem(),
            _ => path.file_name(),
        }
        .ok_or_else(|| anyhow!("Failed to extract basename from {url}"))?
    };

    // Cache directory, e.g. "target/datasets".
    let cache_dir = workspace_dir()
        .join(std::env::var_os("CARGO_TARGET_DIR").unwrap_or_else(|| "target".into()))
        .join("datasets");

    // Cache file path, e.g. "target/datasets/base_full.csr".
    let cache_path = cache_dir.join(basename);

    if cache_path.exists() {
        return Ok(cache_path);
    }

    eprintln!("Downloading {url} to {cache_path:?}...");

    create_dir_all(cache_dir)?;

    let resp = reqwest::blocking::get(url)?;
    if !resp.status().is_success() {
        anyhow::bail!("Failed to download {url}, status: {}", resp.status());
    }
    let total_size = resp.content_length();

    // Download to a temporary file, e.g. "target/datasets/base_full.csr.tmp", to avoid
    // incomplete files.
    let mut tmp_fname = cache_path.clone().into_os_string();
    tmp_fname.push(".tmp");

    // Progress bar.
    let pb = ProgressBar::with_draw_target(total_size, ProgressDrawTarget::stderr_with_hz(12));
    pb.set_style(
        indicatif::ProgressStyle::default_bar()
            .template("{msg} {wide_bar} {bytes}/{total_bytes} (eta:{eta})")
            .expect("failed to set style"),
    );

    // Download + decompress.
    std::io::copy(
        &mut GzDecoder::new(pb.wrap_read(resp)),
        &mut File::create(&tmp_fname)?,
    )?;

    std::fs::rename(&tmp_fname, &cache_path)
        .with_context(|| format!("Failed to rename {tmp_fname:?} to {cache_path:?}"))?;

    Ok(cache_path)
}

fn workspace_dir() -> PathBuf {
    let output = std::process::Command::new(env!("CARGO"))
        .arg("locate-project")
        .arg("--workspace")
        .arg("--message-format=plain")
        .output()
        .unwrap()
        .stdout;
    let cargo_path = Path::new(std::str::from_utf8(&output).unwrap().trim());
    cargo_path.parent().unwrap().to_path_buf()
}
