pluginManagement {
    repositories {
        google()
        mavenCentral()
        gradlePluginPortal()
    }
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        google()
        mavenCentral()
    }
}

rootProject.name = "qdrant-edge-kotlin"

// Single published module: the UniFFI-generated bindings (package
// `io.qdrant.edge`, set in lib/edge/ffi/uniffi.toml) ARE the public API, so
// there is no separate low-level bindings module and no typealias facade — the
// generated Kotlin + native .so + suspend helpers all live in `:qdrant-edge`.
include(":qdrant-edge")
include(":example")
