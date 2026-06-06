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
    id("maven-publish")
    id("signing")
}

// Coordinates are single-sourced from gradle.properties (VERSION_NAME mirrors
// lib/edge/VERSION; GROUP is the Maven group). See gradle.properties.
group = providers.gradleProperty("GROUP").get()
version = providers.gradleProperty("VERSION_NAME").get()

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

    // AGP only produces a publishable `release` software component when a single
    // variant is declared for publishing. Without this, `from(components["release"])`
    // below fails with "component not found".
    publishing {
        singleVariant("release") {
            withSourcesJar()
            // Maven Central requires a -javadoc.jar for every non-pom artifact
            // (aar included; no exemption). AGP generates one from sources here.
            withJavadocJar()
        }
    }
}

// The remote repository is config-phase (it doesn't reference the AGP `release`
// component), so it lives at the top level — not inside afterEvaluate — to stay
// configuration-cache compatible. Creds come from env/Gradle properties at
// release time; `publishToMavenLocal` ignores this block.
publishing {
    repositories {
        maven {
            name = "sonatype"
            url = uri(
                providers.gradleProperty("SONATYPE_URL")
                    .orElse("https://ossrh-staging-api.central.sonatype.com/service/local/staging/deploy/maven2/")
                    .get()
            )
            credentials {
                username = providers.gradleProperty("SONATYPE_USERNAME")
                    .orElse(providers.environmentVariable("SONATYPE_USERNAME"))
                    .orNull
                password = providers.gradleProperty("SONATYPE_PASSWORD")
                    .orElse(providers.environmentVariable("SONATYPE_PASSWORD"))
                    .orNull
            }
        }
    }
}

// AGP creates the `release` component late in configuration, so the publication
// must be wired inside afterEvaluate or `components["release"]` is not yet found.
afterEvaluate {
    publishing {
        publications {
            create<MavenPublication>("release") {
                from(components["release"])

                groupId = project.group.toString()
                artifactId = "qdrant-edge"
                version = project.version.toString()

                pom {
                    name.set("Qdrant Edge")
                    description.set(
                        "Native Android SDK for Qdrant Edge — an in-process, " +
                            "on-device vector search engine."
                    )
                    url.set("https://qdrant.tech/edge/")
                    licenses {
                        license {
                            name.set("Apache License 2.0")
                            url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                        }
                    }
                    developers {
                        developer {
                            id.set("qdrant")
                            name.set("Qdrant Team")
                            email.set("info@qdrant.tech")
                        }
                    }
                    scm {
                        url.set("https://github.com/qdrant/qdrant")
                        connection.set("scm:git:https://github.com/qdrant/qdrant.git")
                        developerConnection.set("scm:git:ssh://git@github.com/qdrant/qdrant.git")
                    }
                }
            }
        }
    }

    // Signing is required by Maven Central but must NOT block local verification
    // (publishToMavenLocal) or CI, where no key is present. Only sign when a key
    // is actually configured.
    signing {
        val signingKey = providers.environmentVariable("SIGNING_KEY").orNull
        val signingPassword = providers.environmentVariable("SIGNING_PASSWORD").orNull
        if (signingKey != null) {
            useInMemoryPgpKeys(signingKey, signingPassword)
            sign(publishing.publications["release"])
        }
    }
}

dependencies {
    api(project(":qdrant-edge-ffi"))
    // For the optional `suspend` convenience wrappers (Coroutines.kt). `api` so
    // consumers calling the suspend functions also get the coroutines types.
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.9.0")
}
