use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow};

pub enum Dataset {
    SpladeWikiMovies,
    NeurIps2023Full,
    NeurIps2023_1M,
    NeurIps2023Small,
    NeurIps2023Queries,
    HMArticles,
}

impl Dataset {
    pub fn download(&self) -> Result<PathBuf> {
        anyhow::bail!("Downloading datasets is not supported on WASM")
    }
}
