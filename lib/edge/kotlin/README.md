# Qdrant Edge — Kotlin (Android)

Kotlin bindings for [Qdrant Edge](https://qdrant.tech/documentation/edge/edge-quickstart/),
built from the shared `qdrant-edge-ffi` Rust crate via [UniFFI](https://github.com/mozilla/uniffi-rs).
Android is the only packaged target today (the ABIs below); the module is named
`kotlin` rather than `android` because the generated bindings are plain Kotlin —
a JVM-desktop artifact could be added later without a second binding surface.

## Supported ABIs

| ABI          | Target triple             | Devices                |
|--------------|---------------------------|------------------------|
| `arm64-v8a`  | `aarch64-linux-android`   | Modern phones/tablets  |
| `x86_64`     | `x86_64-linux-android`    | Emulators (Intel/AMD)  |

> 32-bit targets (`armeabi-v7a`, `x86`) are excluded: upstream Qdrant
> dependencies overflow on 32-bit const evaluation.

## Quick start

```bash
make setup      # Install Rust, cargo-ndk, protobuf; verify NDK
make build      # Cross-compile native libs + generate Kotlin bindings
make aar        # (Optional) Assemble the AAR via Gradle
make size       # Show .so and AAR sizes
```

## Prerequisites

- **Rust** (via `rustup`) + `cargo-ndk`
- **Android NDK** — set `ANDROID_NDK_HOME` or install under `$ANDROID_HOME/ndk/`
- **Protocol Buffers** — `brew install protobuf`
- **Android SDK** — only needed for `make aar`

## Integration

Everything lives in one package — the UniFFI-generated bindings ARE the public
API, so a single import covers every type and sealed-class variant:

```kotlin
import tech.qdrant.edge.*

val shard = EdgeShard.load(path = dataDir, config = config)
val id = PointId.NumId(1u)                 // sealed-class variants work directly —
val query = Query.Nearest(NamedVector.Dense(listOf(0.1f, 0.2f)), using = null)
```

### As a Gradle composite build

Run `make build` in the qdrant checkout first — the generated bindings
(`qdrant_edge_ffi.kt`) and native `.so`s are git-ignored, so a fresh clone has
neither until the build script produces them. Then:

```kotlin
// settings.gradle.kts
includeBuild("path/to/qdrant/lib/edge/kotlin") {
    dependencySubstitution {
        substitute(module("tech.qdrant:qdrant-edge")).using(project(":qdrant-edge"))
    }
}
```

### From Maven Central (once published)

```kotlin
dependencies {
    implementation("tech.qdrant:qdrant-edge:<version>")
}
```

> **Note:** there is no supported "single flat AAR" path. A plain
> `implementation(files("…/qdrant-edge-release.aar"))` does **not** work: the
> AAR bundles the native `.so`s, but a file dependency carries no POM, so
> Gradle never resolves the `api` dependencies (JNA `@aar`, kotlinx-coroutines)
> and it fails at runtime with `NoClassDefFoundError`. Use the composite build
> above for local development, or the published Maven artifact (which carries
> the correct transitive dependencies via its POM).

## Module layout

A single published Gradle module, `:qdrant-edge` — the generated bindings, the
native libraries, and the hand-written suspend helpers all ship together as the
one artifact `tech.qdrant:qdrant-edge`.

```text
kotlin/
├── build-aar.sh               Cross-compile Rust + generate Kotlin bindings
├── Makefile                   setup / build / aar / size / clean
├── settings.gradle.kts
├── build.gradle.kts
├── qdrant-edge/               The published module (import tech.qdrant.edge.*)
│   ├── build.gradle.kts
│   ├── proguard-rules.pro
│   └── src/main/
│       ├── kotlin/tech/qdrant/edge/
│       │   ├── Coroutines.kt        Hand-written suspend wrappers (tracked)
│       │   └── qdrant_edge_ffi.kt   UniFFI-generated bindings (build-aar.sh)
│       └── jniLibs/                 Native .so per ABI (build-aar.sh)
└── example/                   Example Android app
```

The generated `qdrant_edge_ffi.kt` and `jniLibs/*.so` are produced by
`build-aar.sh` and git-ignored; `Coroutines.kt` shares the package but is
hand-written and tracked. There is no separate low-level module and no
typealias facade — the generated Kotlin is the public API directly, so it can
shift with a UniFFI upgrade.

## Public API

The API you use is the domain surface: `EdgeShard`, the request/response data
classes, and the sealed hierarchies (`PointId`, `Vector`, `NamedVector`,
`Query`, `Condition`, `Match`, `UpdateOperation`, …), plus the `suspend`
helpers in `Coroutines.kt`.

The generated package **also** contains UniFFI's low-level plumbing —
`FfiConverter*`, `RustBuffer`, `UniffiLib`, `uniffiEnsureInitialized()`, and
similar `uniffi*` symbols. These are marshalling internals, **not** part of the
supported API; do not reference them. They are `public` because UniFFI's Kotlin
backend emits them that way by design (public visibility is required so
converters can be imported across module boundaries, and `uniffi.toml` offers no
visibility switch). Shipping them as-is matches every published Kotlin UniFFI
artifact (Matrix `sdk-android`, Mozilla Glean / app-services, Bitcoin Dev Kit).

> This differs from the Swift SDK, which demotes its equivalent plumbing to
> `internal`. The asymmetry is intentional — it follows each ecosystem's UniFFI
> convention (the Swift community post-processes visibility; the Kotlin
> community does not).

## Documentation

Every public type and method carries doc comments authored in Rust that
UniFFI propagates to Kotlin KDoc. Hover in Android Studio / IntelliJ for
summaries, error notes, and examples.

## Makefile targets

| Target        | Description                                        |
|---------------|----------------------------------------------------|
| `setup`       | Install all prerequisites                          |
| `build`       | Cross-compile + generate Kotlin bindings (release) |
| `build-debug` | Same, debug mode                                   |
| `aar`         | Build + package AAR via Gradle                     |
| `aar-debug`   | AAR in debug mode                                  |
| `size`        | Show per-ABI .so sizes + AAR size                  |
| `clean`       | Remove build artifacts                             |
| `help`        | Show available targets                             |

## Threading

All `EdgeShard` calls are **synchronous and blocking** (`search`, `query`,
`scroll`, `update`, …) and run on the calling thread. **Never call them on the
main thread** — a large search will trigger an ANR.

The SDK does not impose a dispatcher (you choose where the work runs). For the
heavy operations there are optional `suspend` wrappers in
`tech.qdrant.edge.*` that run the call on a background dispatcher within your
coroutine (default `Dispatchers.IO`, overridable):

```kotlin
import tech.qdrant.edge.searchAsync   // also: queryAsync, scrollAsync, retrieveAsync, updateAsync, optimizeAsync

val hits = shard.searchAsync(request)            // suspends, runs on Dispatchers.IO
val hits = shard.searchAsync(request, myDispatcher)  // or your own pool
```

If you manage your own thread pool, call the plain blocking `shard.search(request)`
instead.

## Lifecycle

`EdgeShard` is `AutoCloseable`; the idiomatic way to scope it is `use { }`,
which releases the native resources when the block exits:

```kotlin
EdgeShard.load(path, config).use { shard ->
    shard.update(UpdateOperation.upsertPoints(points))
    val hits = shard.search(request)
}   // shard disposed here
```

To release *before* disposal (e.g. at app-suspend), call `shard.unload()`
(typically after `shard.flush()`). Do not confuse it with `close()` /
`destroy()`, which dispose the object itself.

## Error handling

Fallible calls throw `EdgeException`, a sealed exception so you can branch on
the error category (the field is `reason`, not `message`):

- `EdgeException.ShardClosed` — the shard was unloaded; reopen it via
  `EdgeShard.load`.
- `EdgeException.InvalidArgument` — host-supplied input was invalid (bad UUID,
  out-of-range vector size, unsupported config, …); fix the input and retry.
- `EdgeException.OperationException` — any other engine failure (I/O, missing
  payload index, dimension mismatch, …).

```kotlin
try {
    EdgeShard.load(path = dataDir, config = config).use { shard ->
        shard.update(operation = upsert)
    } // shard disposed here, even if update throws
} catch (e: EdgeException.ShardClosed) {
    // reopen the shard
} catch (e: EdgeException.InvalidArgument) {
    println("Bad input: ${e.reason}")
} catch (e: EdgeException.OperationException) {
    println("Engine error: ${e.reason}")
}
```
