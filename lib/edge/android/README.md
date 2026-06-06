# Qdrant Edge — Android (Kotlin)

Kotlin/Android bindings for [Qdrant Edge](https://qdrant.tech/documentation/edge/edge-quickstart/),
built from the shared `qdrant-edge-ffi` Rust crate via [UniFFI](https://github.com/mozilla/uniffi-rs).

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

Consumers import from the public facade package only:

```kotlin
import tech.qdrant.edge.*

val shard = EdgeShard.load(path = dataDir, config = config)
```

> **Note:** the `tech.qdrant.edge.*` facade re-exports every public type via a
> `typealias`, but a `typealias` cannot re-export **sealed-class variants**. To
> construct one — e.g. `PointId.NumId(1u)`, `Vector.Single(...)`,
> `Query.Nearest(...)`, `Match.Value(...)` — also add `import
> tech.qdrant.edge.ffi.*`. (The variants are identical types; only the
> constructor reference needs the `ffi` import.)

### As a Gradle composite build

```kotlin
// settings.gradle.kts
includeBuild("path/to/qdrant/lib/edge/android") {
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
> `implementation(files("…/qdrant-edge-release.aar"))` does **not** work: an
> Android library module's `assembleRelease` produces an AAR that bundles
> neither the `:qdrant-edge-ffi` module's native `.so`/generated bindings nor
> the JNA `@aar` dependency, so it fails at runtime with `UnsatisfiedLinkError`
> / `NoClassDefFoundError`. Use the composite build above for local development,
> or the published Maven artifact (which carries the correct transitive
> dependencies via its POM).

## Module layout

```
android/
├── build-aar.sh               Cross-compile Rust + generate Kotlin bindings
├── Makefile                   setup / build / aar / size / clean
├── settings.gradle.kts
├── build.gradle.kts
├── qdrant-edge/               Public facade (import this)
│   ├── build.gradle.kts
│   └── src/main/kotlin/tech/qdrant/edge/PublicApi.kt
├── qdrant-edge-ffi/           Internal module (UniFFI bindings + .so files)
│   ├── build.gradle.kts
│   ├── proguard-rules.pro
│   └── src/main/
│       ├── jniLibs/           Populated by build-aar.sh
│       └── kotlin/            Populated by build-aar.sh
└── example/                   Example Android app
```

`:qdrant-edge` holds the public API (`EdgeShard`, `Point`, `Query`, …) as
typealiases onto `tech.qdrant.edge.ffi.*`. `:qdrant-edge-ffi` is an
implementation detail and may change on every UniFFI upgrade.

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
import tech.qdrant.edge.searchAsync   // also: queryAsync, scrollAsync, retrieveAsync

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
