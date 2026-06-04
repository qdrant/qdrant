// Public facade module. Re-exports `tech.qdrant.edge.ffi.*` (UniFFI-generated)
// as `tech.qdrant.edge.*` via typealiases, hiding FFI plumbing from consumers.

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
