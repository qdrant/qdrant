// The single Qdrant Edge Android module: UniFFI-generated Kotlin bindings
// (package `tech.qdrant.edge`, set in lib/edge/ffi/uniffi.toml) + the native
// .so libraries + idiomatic suspend helpers (Coroutines.kt). The generated
// bindings ARE the public API — no typealias facade (it drifted, couldn't
// re-export sealed-class variants, and leaked the raw package via `api(...)`).
// Published as the single artifact `tech.qdrant:qdrant-edge`.
//
// The generated file also carries UniFFI's `public` plumbing (FfiConverter*,
// RustBuffer, uniffi*). Unlike the Swift SDK (which demotes its equivalents to
// `internal`), we ship it public — the deliberate UniFFI-Kotlin design and the
// norm of every published Kotlin UniFFI artifact; see uniffi.toml and the
// README's "Public API" note. It is documented as non-API, not intended for
// consumers.

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
        consumerProguardFiles("proguard-rules.pro")
        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
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

    // Fail fast on a mis-provisioned RELEASE: signing is best-effort above, so a
    // release with credentials but no key would otherwise upload UNSIGNED
    // artifacts and only fail late at Central. A real release supplies Sonatype
    // credentials, so treat "credentials present, signing key absent" as a
    // configuration error. Config-time + provider-based (no taskGraph hook), so
    // it stays configuration-cache compatible; local/CI builds without
    // credentials (and publishToMavenLocal) are unaffected.
    val hasSonatypeCreds = (
        providers.gradleProperty("SONATYPE_USERNAME").orNull
            ?: providers.environmentVariable("SONATYPE_USERNAME").orNull
        ) != null
    val hasSigningKey = providers.environmentVariable("SIGNING_KEY").orNull != null
    if (hasSonatypeCreds && !hasSigningKey) {
        throw GradleException(
            "SONATYPE_USERNAME is set but SIGNING_KEY is not — Maven Central " +
                "rejects unsigned artifacts. Set SIGNING_KEY (and SIGNING_PASSWORD) " +
                "to publish a signed release."
        )
    }
}

dependencies {
    // JNA carries the UniFFI-generated bindings' calls into the packaged native
    // .so. `@aar` pulls the Android artifact (with its own native libs layout);
    // `api` so a consumer that drops to raw JNA types can still resolve them.
    api("net.java.dev.jna:jna:5.14.0@aar")
    // For the optional `suspend` convenience wrappers (Coroutines.kt). `api` so
    // consumers calling the suspend functions also get the coroutines types.
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.9.0")

    androidTestImplementation("androidx.test.ext:junit:1.2.1")
    androidTestImplementation("androidx.test:runner:1.6.2")
}
