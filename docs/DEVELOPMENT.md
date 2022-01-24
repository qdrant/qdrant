
# Developer's guide to Qdrant


## Profiling

There are several benchmarks implemented in Qdrant. Benchmarks are not included in CI/CD and might take some time to execute.
So the expected approach to benchmarking is to run only ones which might be affected by your changes.

To run benchmark, use the following command inside a related sub-crate:

```bash
cargo bench --bench name_of_banchmark
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
cargo bench --bench name_of_banchmark -- --profile-time=60
```

This command will run each benchmark iterator for `60` seconds and generate FlameGraph svg along with profiling records file.
These records could later be used to generate visualisation of the call-graph.

![FlameGraph example](./imgs/flamegraph-profile.png)

Use [pprof](https://github.com/google/pprof) and the following command to generate `svg` with a call graph:

```bash
~/go/bin/pprof -output=profile.svg -svg ${qdrant_root}/target/criterion/${benchmark_name}/${function_name}/profile/profile.pb
```

![call-graph example](./imgs/call-graph-profile.png)

## API changes

Qdrant uses the [openapi](https://spec.openapis.org/oas/latest.html) specification to document its API.

This means changes to the API must be followed by changes to the specification.

Here is a quick step-by-step guide:

1. code endpoints and model in Rust
2. update integration tests `/tests/basic_api_test.sh`
3. change specs in `/openapi/*ytt.yaml`
4. add new schema definitions to `src/schema_generator.rs`
5. run `/tools/generate_openapi_models.sh` to generate specs
6. move newly created openapi.json `cp docs/redoc/openapi.json docs/redoc/master/openapi.json`
7. expose file by starting an HTTP server, for instance `python -m http.server`, in `/docs/redoc`
8. validate specs by browsing redoc on `http://localhost:8000/?v=master`
9. validate `openapi-merged.yaml` using [swagger editor](https://editor.swagger.io/)