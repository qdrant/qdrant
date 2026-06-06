// Bindings module: UniFFI-generated Kotlin + native .so libraries. Most
// consumers depend on :qdrant-edge, but this module is published too (as
// `tech.qdrant:qdrant-edge-ffi`) so that :qdrant-edge's POM can reference it as
// a resolvable Maven dependency — a project dependency would otherwise serialize
// as `version: unspecified` and break the published POM.

plugins {
    id("com.android.library")
    id("org.jetbrains.kotlin.android")
    id("maven-publish")
    id("signing")
}

// Coordinates single-sourced from gradle.properties (see :qdrant-edge).
group = providers.gradleProperty("GROUP").get()
version = providers.gradleProperty("VERSION_NAME").get()

android {
    namespace = "tech.qdrant.edge.ffi"
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

    publishing {
        singleVariant("release") {
            withSourcesJar()
        }
    }
}

afterEvaluate {
    publishing {
        publications {
            create<MavenPublication>("release") {
                from(components["release"])

                groupId = project.group.toString()
                artifactId = "qdrant-edge-ffi"
                version = project.version.toString()

                pom {
                    name.set("Qdrant Edge FFI bindings")
                    description.set(
                        "UniFFI-generated Kotlin bindings and native libraries " +
                            "for Qdrant Edge. Depend on tech.qdrant:qdrant-edge instead."
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
    // `api` so consuming modules can reach into `tech.qdrant.edge.ffi.*` if
    // they need the raw UniFFI types (advanced use only).
    api("net.java.dev.jna:jna:5.14.0@aar")

    androidTestImplementation("androidx.test.ext:junit:1.2.1")
    androidTestImplementation("androidx.test:runner:1.6.2")
}
