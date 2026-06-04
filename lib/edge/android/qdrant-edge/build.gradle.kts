// Public facade module. Re-exports `tech.qdrant.edge.ffi.*` (UniFFI-generated)
// as `tech.qdrant.edge.*` via typealiases for a clean import surface.
//
// NOTE: this does NOT fully hide the ffi package. The dependency below is
// `api(...)` (required so the public typealiases resolve), which transitively
// exposes `tech.qdrant.edge.ffi.*` on the consumer's compile classpath. A
// Kotlin typealias also cannot carry a sealed class's nested variants
// (`PointId.NumId`, `Query.Nearest`), so code constructing those still imports
// `tech.qdrant.edge.ffi.*` directly. Treat the facade as an ergonomic alias
// layer, not an enforced encapsulation boundary. (The Swift SDK *does* enforce
// hiding via the demote pass + the SwiftPM product exposing only `QdrantEdge`.)

plugins {
    id("com.android.library")
    id("org.jetbrains.kotlin.android")
}

android {
    namespace = "tech.qdrant.edge"
    compileSdk = 34

    defaultConfig {
        minSdk = 24
    }

    buildTypes {
        release {
            isMinifyEnabled = false
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8
    }

    kotlinOptions {
        jvmTarget = "1.8"
    }
}

dependencies {
    api(project(":qdrant-edge-ffi"))
    // For the optional `suspend` convenience wrappers (Coroutines.kt). `api` so
    // consumers calling the suspend functions also get the coroutines types.
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.9.0")
}
