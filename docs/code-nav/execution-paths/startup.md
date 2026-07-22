# Qdrant process startup

## Overview

Qdrant’s **`main`** in `qdrant/src/main.rs` parses CLI args, loads **`Settings`**, initializes logging and global subsystem options, loads or creates **persistent consensus / peer state**, optionally recovers from **snapshots**, constructs **Tokio runtimes** (search, update, general) and a shared **`ResourceBudget`** for optimizers, builds **`TableOfContent`** (loading all collections from disk), wires **cluster consensus** when enabled, then starts **REST** (Actix) and **gRPC** (Tonic) server threads and blocks on their **`JoinHandle`s** until those threads exit.

Configuration types and defaults begin in `qdrant/src/settings.rs` (including `ServiceConfig` with `http_port`, optional `grpc_port`, optional `metrics_port`).

## Startup sequence

### 1. Configuration

- **`Args::parse()`** (`clap::Parser` on the `Args` struct in `qdrant/src/main.rs`): bootstrap URI, peer URI, snapshot paths, `config_path`, telemetry flags, `reinit`, etc.
- **`Settings::new(args.config_path)?`**: layered config (default embedded YAML, file, environment). See `qdrant/src/settings.rs`.
- **`init_feature_flags(settings.feature_flags)`** — feature flags from config (`qdrant/src/main.rs`).
- **Tracing / logging**: `tracing::setup(...)` with `settings.logger` and `settings.log_level`; earlier execution has no structured logging.
- **Globals**: `set_global(settings.storage.mmap_advice)`, `segment::vector_storage::common::set_async_scorer(...)`, optional **GPU** init when `feature = "gpu"`, **`welcome`**, audit logger (`common::audit::init_audit_logger`), **`settings.validate_and_warn()`**.
- **Storage path**: `fs::create_dir_all(&settings.storage.storage_path)`; **filesystem / mmap checks** via `common::fs::check_fs_info`, `check_mmap_functionality`, result stored in `MULTI_MMAP_SUPPORT_CHECK_RESULT`.

### 2. Persistent state

- **`Persistent::load_or_init`** (`storage::content_manager::consensus::persistent::Persistent`) under `settings.storage.storage_path`: loads or initializes consensus-related persisted state (peer id from `settings.cluster.peer_id`, bootstrap vs first-peer semantics, `reinit` behavior as documented on the `Args::reinit` flag in `main.rs`).

### 3. Runtimes

- **`create_search_runtime(settings.storage.performance.max_search_threads)`**
- **`create_update_runtime(settings.storage.performance.max_optimization_runtime_threads)`**
- **`create_general_purpose_runtime()`** — `runtime_handle` cloned for async work across the process.
- **Optimizer budget**: `get_cpu_budget` / `get_io_budget` from `common::cpu` / `common::budget`, then **`ResourceBudget::new(cpu_budget, io_budget)`** (`qdrant/src/main.rs`).

### 4. TableOfContent

- **`ChannelService::new`** for inter-peer HTTP/TLS settings; in distributed mode, **`TransportChannelPool`**, timeouts, TLS client config, and peer id → address/metadata maps from **`persistent_consensus_state`** are attached (`main.rs`).
- **`TableOfContent::new`** (`storage::content_manager::toc::TableOfContent` in `qdrant/lib/storage/src/content_manager/toc/mod.rs`):
  - Ensures `collections/` (and optional temp path) exist.
  - Scans collection directories with valid config, loads each **`Collection::load`** concurrently (`load_concurrency`), on **`general_runtime`**.
  - Opens **`AliasPersistence`** at `storage_path/aliases` (`ALIASES_PATH`).
  - Stores search/update/general runtimes, **`optimizer_resource_budget`**, **`this_peer_id`**, optional **`consensus_proposal_sender`** (`OperationSender` when cluster enabled).
- **`toc.clear_all_tmp_directories()`** after construction (`main.rs`).
- **`runtime_handle.block_on`** logs loaded collection names via **`toc.all_collections`**.

### 5. Consensus (cluster mode)

When **`settings.cluster.enabled`** (`is_distributed_deployment`):

- **`ConsensusManager::new(...)`** → **`ConsensusStateRef`**; **`Dispatcher::with_consensus`**; **`TocDispatcher`** registered on the ToC (`main.rs`).
- **`HealthChecker::spawn`** for readiness/bootstrap-related health (`common::health`).
- **`Consensus::run`** in a **dedicated thread** (`std::thread`): Raft and cluster messaging, consuming **`propose_receiver`** and using **`channel_service`**, **`settings`**, telemetry collectors, **`toc_arc`**, `reinit` flag.
- Async tasks: e.g. cancel shard transfers after leader establishment.
- **`handle_existing_collections`** when recovering collections into consensus on new deployments or after snapshot restore (`migrations::single_to_cluster`).

Single-node: logs “Distributed mode disabled”; dispatcher without consensus; `health_checker = None`; no `Consensus::run` thread.

### 6. Servers

- **Telemetry reporter** (`TelemetryReporter::run`) if enabled; **issues subscribers**, **request profile collector**, **InferenceService::init_global**.
- **REST**: new thread `"web"` → **`actix::init`** (`qdrant/src/actix/mod.rs`): `actix_web::rt::System::new().block_on`, **`HttpServer`**, bind / TLS, **`server.run().await`**.
- **Metrics** (optional): thread `"metrics"` → **`actix::metrics_service::init_metrics`** when `settings.service.metrics_port` is set (`settings.rs` documents separate `/metrics` port).
- **gRPC** (optional): thread `"grpc"` → **`tonic::init`** when `settings.service.grpc_port` is **`Some`**; otherwise log that gRPC is disabled.

**`touch_started_file_indicator()`** after servers are spawned (`qdrant/src/startup`).

**Main thread:** collects **`JoinHandle<Result<(), Error>>`** for web / metrics / grpc / consensus and **`join()`**s each in order; then drops **`toc_arc`** and **`settings`**.

## Shutdown

- **gRPC** (`qdrant/src/tonic/mod.rs`): public (and internal) servers use **`serve_with_shutdown`**. **`wait_stop_signal`** waits for **SIGTERM** or **SIGINT** on Unix (`tokio::signal::unix`), or **Ctrl+C** on non-Unix; completing that future triggers graceful server shutdown.
- **REST**: Actix **`HttpServer::run().await`** completes when the Actix system stops (typically signal-driven depending on Actix / runtime setup). The web thread returns **`io::Result`** to **`main`**, which propagates **`?`** after **`join`**.
- **Consensus** thread: exits when the consensus **`run`** future completes (cluster mode).
- There is no separate explicit “shutdown hook” block in `main.rs` after joins beyond dropping globals; graceful behavior is primarily **server-driven** (signals → stop accepting → drain).

## Cross-references

| Topic | Location |
|--------|-----------|
| CLI, main flow, runtimes, ToC, consensus, threads | `qdrant/src/main.rs` |
| Settings / service ports | `qdrant/src/settings.rs` |
| REST server | `qdrant/src/actix/mod.rs`, `qdrant/src/actix/metrics_service.rs` |
| gRPC + signal shutdown | `qdrant/src/tonic/mod.rs` |
| ToC construction, collection load, aliases | `qdrant/lib/storage/src/content_manager/toc/mod.rs` |
| Persistent peer / consensus state | `storage::content_manager::consensus::persistent::Persistent` |
| Consensus thread entry | `qdrant/src/consensus` (referenced from `main.rs` as `Consensus::run`) |
| Runtime helpers | `qdrant/src/common/helpers.rs` (`create_*_runtime`) |
| Started-file indicator | `qdrant/src/startup` (`touch_started_file_indicator`, `remove_started_file_indicator`) |
