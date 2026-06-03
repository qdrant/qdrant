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
