use ::tonic::transport::Uri;
use clap::Parser;

/// Qdrant (read: quadrant ) is a vector similarity search engine.
/// It provides a production-ready service with a convenient API to store, search, and manage points - vectors with an additional payload.
///
/// This CLI starts a Qdrant peer/server.
#[derive(Parser, Debug)]
#[command(version, about)]
pub(crate) struct Args {
    /// Uri of the peer to bootstrap from in case of multi-peer deployment.
    /// If not specified - this peer will be considered as a first in a new deployment.
    #[arg(long, value_parser, value_name = "URI")]
    pub(crate) bootstrap: Option<Uri>,
    /// Uri of this peer.
    /// Other peers should be able to reach it by this uri.
    ///
    /// This value has to be supplied if this is the first peer in a new deployment.
    ///
    /// In case this is not the first peer and it bootstraps the value is optional.
    /// If not supplied then qdrant will take internal grpc port from config and derive the IP address of this peer on bootstrap peer (receiving side)
    #[arg(long, value_parser, value_name = "URI")]
    pub(crate) uri: Option<Uri>,

    /// Force snapshot re-creation
    /// If provided - existing collections will be replaced with snapshots.
    /// Default is to not recreate from snapshots.
    #[arg(short, long, action, default_value_t = false)]
    pub(crate) force_snapshot: bool,

    /// List of paths to snapshot files.
    /// Format: <snapshot_file_path>:<target_collection_name>
    ///
    /// WARN: Do not use this option if you are recovering collection in existing distributed cluster.
    /// Use `/collections/<collection-name>/snapshots/recover` API instead.
    #[arg(long, value_name = "PATH:NAME", alias = "collection-snapshot")]
    pub(crate) snapshot: Option<Vec<String>>,

    /// Path to snapshot of multiple collections.
    /// Format: <snapshot_file_path>
    ///
    /// WARN: Do not use this option if you are recovering collection in existing distributed cluster.
    /// Use `/collections/<collection-name>/snapshots/recover` API instead.
    #[arg(long, value_name = "PATH")]
    pub(crate) storage_snapshot: Option<String>,

    /// Path to an alternative configuration file.
    /// Format: <config_file_path>
    ///
    /// Default path : config/config.yaml
    #[arg(long, value_name = "PATH")]
    pub(crate) config_path: Option<String>,

    /// Disable telemetry sending to developers
    /// If provided - telemetry collection will be disabled.
    /// Read more: <https://qdrant.tech/documentation/guides/telemetry>
    #[arg(long, action, default_value_t = false)]
    pub(crate) disable_telemetry: bool,
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;

    #[test]
    fn test_cmd() {
        Args::command().debug_assert();
    }

    #[test]
    fn test_cmd_ui() {
        let t = trycmd::TestCases::new();
        let qdrant_binary = trycmd::cargo::cargo_bin("qdrant");
        t.register_bin("qdrant", &qdrant_binary);
        t.case("tests/cli/help.toml");
    }
}
