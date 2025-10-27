
# Developer's guide to Qdrant


## Build Qdrant

### Docker 🐳

Build your own from source

```bash
docker build . --tag=qdrant/qdrant
```

Or use latest pre-built image from [DockerHub](https://hub.docker.com/r/qdrant/qdrant)

```bash
docker pull qdrant/qdrant
```

To run the container, use the command:

```bash
docker run -p 6333:6333 qdrant/qdrant
```

And once you need a fine-grained setup, you can also define a storage path and custom configuration:

```bash
docker run -p 6333:6333 \
    -v $(pwd)/path/to/data:/qdrant/storage \
    -v $(pwd)/path/to/snapshots:/qdrant/snapshots \
    -v $(pwd)/path/to/custom_config.yaml:/qdrant/config/production.yaml \
    qdrant/qdrant
```

* `/qdrant/storage` - is the place where Qdrant persists all your data.
Make sure to mount it as a volume, otherwise docker will drop it with the container.
- `/qdrant/snapshots` - is the place where Qdrant stores [snapshots](https://qdrant.tech/documentation/concepts/snapshots/)
* `/qdrant/config/production.yaml` - is the file with engine configuration. You can override any value from the [reference config](https://github.com/qdrant/qdrant/blob/master/config/config.yaml)

Now Qdrant should be accessible at [localhost:6333](http://localhost:6333/).

#### Docker image build parameters

As it was mentioned before, you can build your own Qdrant docker image with

```bash
docker build . --tag=qdrant/qdrant
```

You may also configure certain Docker arguments to fine tune your image:

+ `FEATURES` -- define cargo features;
+ `RUSTFLAGS` -- define `rustc` compilation flags;
+ `LINKER` -- define linker in `RUSTFLAGS`;
+ `TARGET_CPU` -- define `target-cpu` in `RUSTFLAGS`;
+ `JEMALLOC_SYS_WITH_LG_PAGE` -- define jemalloc's page size in base 2 log; may be
  useful when compiling for some ARM machines; it corresponds to jemalloc's
  `--with-lg-page` configure argument.
+ `GPU` -- add GPU support, either `nvidia` or `amd`.

For example:

```bash
docker build . --tag=qdrant/qdrant \
    --build-arg TARGET_CPU=native --build-arg GPU=amd
```

### Local development
#### Linux/Debian/MacOS
To run Qdrant on local development environment you need to install below:
- Install Rust, follow: [install rust](https://www.rust-lang.org/tools/install)
- Install `rustfmt` toolchain for Rust
    ```shell
    rustup component add rustfmt
    ```
- Install dependencies:
    ```shell
    sudo apt-get update -y
    sudo apt-get upgrade -y
    sudo apt-get install -y curl unzip gcc-multilib \
        clang cmake jq \
        g++-9-aarch64-linux-gnu \
        gcc-9-aarch64-linux-gnu
    ```
- Install `protoc` from source
    ```shell
    PROTOC_VERSION=22.2
    PKG_NAME=$(uname -s | awk '{print ($1 == "Darwin") ? "osx-universal_binary" : (($1 == "Linux") ? "linux-x86_64" : "")}')

    # curl `proto` source file
    curl -LO https://github.com/protocolbuffers/protobuf/releases//download/v$PROTOC_VERSION/protoc-$PROTOC_VERSION-$PKG_NAME.zip

    unzip protoc-$PROTOC_VERSION-$PKG_NAME.zip -d $HOME/.local

    export PATH="$PATH:$HOME/.local/bin"

    # remove source file if not needed
    rm protoc-$PROTOC_VERSION-$PKG_NAME.zip

    # check installed `protoc` version
    protoc --version
    ```
- Build and run the app
    ```shell
    cargo build --release --bin qdrant

    ./target/release/qdrant
    ```
- Install Python dependencies for testing
    ```shell
    poetry -C tests install --sync
    ```
    Then you could use `poetry -C run pytest tests/openapi` and `poetry -C run pytest tests/consensus_tests` to run the tests.
- Use the web UI

    Web UI repo is [in a separate repo](https://github.com/qdrant/qdrant-web-ui), but there's a utility script to sync it to the `static` folder:
    ```shell
    ./tools/sync-web-ui.sh
    ```

### Nix/NixOS
If you are using [Nix package manager](https://nixos.org/) (available for Linux and MacOS), you can run `nix-shell` in the project root to get a shell with all dependencies installed.
It includes dependencies to build Rust code as well as to run Python tests and various tools in the `./tools` directory.

## Profiling

There are several benchmarks implemented in Qdrant. Benchmarks are not included in CI/CD and might take some time to execute.
So the expected approach to benchmarking is to run only ones which might be affected by your changes.

To run benchmark, use the following command inside a related sub-crate:

```bash
cargo bench --bench name_of_benchmark
```

In this case you will see the execution timings and, if you launched this bench earlier, the difference in execution time.

Example output:

```
scoring-vector/basic-score-point
                        time:   [111.81 us 112.07 us 112.31 us]
                        change: [+19.567% +20.454% +21.404%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 9 outliers among 100 measurements (9.00%)
  3 (3.00%) low severe
  3 (3.00%) low mild
  2 (2.00%) high mild
  1 (1.00%) high severe
scoring-vector/basic-score-point-10x
                        time:   [111.86 us 112.44 us 113.04 us]
                        change: [-1.6120% -0.5554% +0.5103%] (p = 0.32 > 0.05)
                        No change in performance detected.
Found 1 outliers among 100 measurements (1.00%)
  1 (1.00%) high mild
```


### FlameGraph and call-graph visualisation
To run benchmarks with profiler to generate FlameGraph - use the following command:

```bash
cargo bench --bench name_of_benchmark -- --profile-time=60
```

This command will run each benchmark iterator for `60` seconds and generate FlameGraph svg along with profiling records files.
These records could later be used to generate visualisation of the call-graph.

![FlameGraph example](./imgs/flamegraph-profile.png)

Use [pprof](https://github.com/google/pprof) and the following command to generate `svg` with a call graph:

```bash
~/go/bin/pprof -output=profile.svg -svg ${qdrant_root}/target/criterion/${benchmark_name}/${function_name}/profile/profile.pb
```

![call-graph example](./imgs/call-graph-profile.png)

### Coverage reports

We generate coverage reports every day that can be accessed [here](https://app.codecov.io/gh/qdrant/qdrant/tree/code-coverage)

Note: These reports **only cover the Rust unit tests** (for now)

![CI coverage report](./imgs/ci-coverage-report.png)

You can also generate coverage reports locally with the following commands

```bash
cd qdrant
cargo install cargo-llvm-cov
./tools/coverage.sh

cd target/llvm-cov/html
python -m http.server
open http://localhost:8000
```

![Local coverage report](./imgs/local-coverage-report.png)

### Tango.rs-based benchmarks

Some benchmarks are implemented using the [Tango.rs](https://github.com/bazhenov/tango) framework.
It enables more precise comparisons between two revisions of the code by running them simultaneously.
Basic usage:

1. Compile and run the baseline version in the `solo` mode:
   ```console
   $ cargo bench -p common --bench bitpacking_tango -- solo
       Finished `bench` profile [optimized + debuginfo] target(s) in 0.22s
        Running benches/bitpacking_tango.rs (target/release/deps/bitpacking_tango-9713980dd08cde85)
   bitpacking/read                                     [  30.8 ns ...  43.9 ns ... 125.3 ns ]  stddev:   6.7 ns
   bitpacking/write                                    [  32.4 ns ...  50.1 ns ...  91.1 ns ]  stddev:   7.3 ns
   bitpacking_links/read                               [ 343.3 ns ... 378.3 ns ... 419.4 ns ]  stddev:  16.5 ns
   ```

2. Note the binary name in the output above. Copy it to compare against, or use the `cargo-export` tool to automate this step.
   ```console
   $ cp target/release/deps/bitpacking_tango-9713980dd08cde85 ./baseline
   ```

3. Change the code and run the benchmark in the `compare` mode to compare the performance against the baseline.
   ```console
   $ cargo bench -p common --bench bitpacking_tango -- compare ./baseline
       Finished `bench` profile [optimized + debuginfo] target(s) in 0.14s
        Running benches/bitpacking_tango.rs (target/release/deps/bitpacking_tango-9713980dd08cde85)
   bitpacking/read                                    [  41.8 ns ...  41.9 ns ]      +0.08%
   bitpacking/write                                   [  45.7 ns ...  45.5 ns ]      -0.46%
   bitpacking_links/read                              [ 369.4 ns ... 368.4 ns ]      -0.27%
   ```

### Real-time profiling

Qdrant have basic [`tracing`] support with [`Tracy`] profiler and [`tokio-console`] integrations
that can be enabled with optional features.

- [`tracing`] is an _optional_ dependency that can be enabled with `tracing` feature
- `tracy` feature enables [`Tracy`] profiler integration
- `console` feature enables [`tokio-console`] integration
  - note, that you'll also have to [pass `--cfg tokio_unstable` arguments to `rustc`][tokio-tracing] to enable this feature
  - by default [`tokio-console`] binds to `127.0.0.1:6669`
  - if you want to connect [`tokio-console`] to Qdrant instance running inside a Docker container
    or on remote server, you can define `TOKIO_CONSOLE_BIND` when running Qdrant to override it
    (e.g., `TOKIO_CONSOLE_BIND=0.0.0.0:6669` to listen on all interfaces)
- `tokio-tracing` feature explicitly enables [`Tokio` crate tracing][tokio-tracing]
  - note, that you'll also have to [pass `--cfg tokio_unstable` arguments to `rustc`][tokio-tracing] to enable this feature
  - this is required (and enabled automatically) by the `console` feature
  - but you can enable it explicitly with the `tracy` feature, to see Tokio traces in [`Tracy`] profiler

Qdrant code is **not** instrumented by default, so you'll have to manually add `#[tracing::instrument]` attributes
on functions and methods that you want to profile.

Qdrant uses [`tracing-log`] as the [`log`] backend, so `log` and `log-always` features of the [`tracing`] crate
[should _not_ be enabled][tracing-log-warning]!

```rust
// `tracing` crate is an *optional* dependency in `lib/*` crates, so if you want the code to compile
// when `tracing` feature is disabled, you have to use `#[cfg_attr(...)]`...
//
// See https://doc.rust-lang.org/reference/conditional-compilation.html#the-cfg_attr-attribute
#[cfg_attr(feature = "tracing", tracing::instrument)]
fn my_function(some_parameter: String) {
    // ...
}

// ...or if you just want to do some quick-and-dirty profiling, you can use `#[tracing::instrument]`
// directly, just don't forget to add `--features tracing` when running `cargo` (or add `tracing`
// to default features in `Cargo.toml`)
#[tracing::instrument]
fn some_other_function() {
    // ...
}
```

[`tracing`]: https://docs.rs/tracing/latest/tracing/
[`Tracy`]: https://github.com/wolfpld/tracy
[`tokio-console`]: https://docs.rs/tokio-console/latest/tokio_console/
[tokio-tracing]: https://docs.rs/tokio/latest/tokio/#unstable-features
[`tracing-log`]: https://docs.rs/tracing-log/latest/tracing_log/
[`log`]: https://docs.rs/log/latest/log/
[tracing-log-warning]: https://docs.rs/tracing-log/latest/tracing_log/#caution-mixing-both-conversions

## API changes

### REST

Qdrant uses the [openapi](https://spec.openapis.org/oas/latest.html) specification to document its API.

This means changes to the API must be followed by changes to the specification.
This is enforced by CI.

Here is a quick step-by-step guide:

1. code endpoints and model in Rust
2. change specs in `/openapi/*ytt.yaml`
3. add new schema definitions to `src/schema_generator.rs`
4. run `./tools/generate_openapi_models.sh` to generate specs
5. update integration tests `tests/openapi` and run them with `pytest tests/openapi` (use poetry or nix to get `pytest`)
6. expose file by starting an HTTP server, for instance `python -m http.server`, in `/docs/redoc`
7. validate specs by browsing redoc on `http://localhost:8000/?v=master`
8. validate `openapi-merged.yaml` using [swagger editor](https://editor.swagger.io/)

### gRPC

Qdrant uses [tonic](https://github.com/hyperium/tonic) to serve gRPC traffic.

Our protocol buffers are defined in `lib/api/src/grpc/proto/*.proto`

1. define request and response types using protocol buffers (use [oneof](https://developers.google.com/protocol-buffers/docs/proto3#oneof) for enums payloads)
2. specify RPC methods inside the service definition using protocol buffers
3. `cargo build` or `cargo build -p api` will generate the struct definitions and a service trait
4. implement the service trait in Rust
5. start server `cargo run --bin qdrant`
6. run integration test `./tests/basic_grpc_test.sh`

Here is a good [tonic tutorial](https://github.com/hyperium/tonic/blob/master/examples/routeguide-tutorial.md#defining-the-service) for reference.

### System integration

On top of the API definitions, Qdrant has a few system integrations that need to be considered when making changes:
1. add new endpoints to the metrics allow lists in `src/common/metrics.rs`
2. test the JWT integration in `tests/auth_tests`
