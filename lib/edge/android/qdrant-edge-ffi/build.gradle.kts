// Internal module: UniFFI-generated Kotlin bindings + native .so libraries.
// Consumers depend on :qdrant-edge, not this module directly.

plugins {
    id("com.android.library")
    id("org.jetbrains.kotlin.android")
}

android {
    namespace = "tech.qdrant.edge.ffi"
    compileSdk = 34

    defaultConfig {
        minSdk = 24
        consumerProguardFiles("proguard-rules.pro")
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
    // `api` so consuming modules can reach into `tech.qdrant.edge.ffi.*` if
    // they need the raw UniFFI types (advanced use only).
    api("net.java.dev.jna:jna:5.14.0@aar")
}
