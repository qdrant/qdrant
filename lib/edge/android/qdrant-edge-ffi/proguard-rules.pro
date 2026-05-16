# Consumer ProGuard rules for :qdrant-edge-ffi.
#
# R8 under `isMinifyEnabled = true` would otherwise strip classes that JNA
# and the UniFFI-generated bindings reach through reflection.

-dontwarn java.awt.*
-keep class com.sun.jna.** { *; }
-keep class * implements com.sun.jna.** { *; }
-keep class tech.qdrant.edge.ffi.** { *; }
