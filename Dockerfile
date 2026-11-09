# Enable GPU support.
# This option can be set to `nvidia` or `amd` to enable GPU support.
# This option is defined here because it is used in `FROM` instructions.
ARG GPU

# Cross-compiling using Docker multi-platform builds/images and `xx`.
#
# https://docs.docker.com/build/building/multi-platform/
# https://github.com/tonistiigi/xx
FROM --platform=${BUILDPLATFORM:-linux/amd64} tonistiigi/xx AS xx

# Utilizing Docker layer caching with `cargo-chef`.
#
# https://www.lpalmieri.com/posts/fast-rust-docker-builds/

# Note: builder's Debian release must not be newer than the runtime images' (`debian:13-slim`),
# otherwise the binary may require a newer glibc than the runtime images provide.
# See: <https://github.com/qdrant/qdrant/pull/7334>
FROM --platform=${BUILDPLATFORM:-linux/amd64} lukemathwalker/cargo-chef:latest-rust-1.99.1-bookworm  AS chef


FROM chef AS planner
WORKDIR /qdrant
COPY . .
RUN cargo chef prepare --recipe-path recipe.json


FROM chef AS builder
WORKDIR /qdrant

COPY --from=xx / /

# Relative order of `ARG` and `RUN` commands in the Dockerfile matters.
#
# If you pass a different `ARG` to `docker build`, it would invalidate Docker layer cache
# for the next steps. (E.g., the following steps may depend on a new `ARG` value, so Docker would
# have to re-execute them instead of using a cached layer from a previous run.)
#
# Steps in this stage are ordered in a way that should maximize Docker layer cache utilization,
# so, please, don't reorder them without prior consideration. 🥲

RUN apt-get update \
    && apt-get install -y clang lld cmake protobuf-compiler jq \
    && rustup component add rustfmt \
    && cargo install cargo-sbom

# `ARG`/`ENV` pair is a workaround for `docker build` backward-compatibility.
#
# https://github.com/docker/buildx/issues/510
ARG BUILDPLATFORM
ENV BUILDPLATFORM=${BUILDPLATFORM:-linux/amd64}

ARG MOLD_VERSION=2.41.0

RUN case "$BUILDPLATFORM" in \
        */amd64 ) PLATFORM=x86_64 ;; \
        */arm64 | */arm64/* ) PLATFORM=aarch64 ;; \
        * ) echo "Unexpected BUILDPLATFORM '$BUILDPLATFORM'" >&2; exit 1 ;; \
    esac; \
    \
    mkdir -p /opt/mold; \
    cd /opt/mold; \
    \
    TARBALL="mold-$MOLD_VERSION-$PLATFORM-linux.tar.gz"; \
    curl -sSLO "https://github.com/rui314/mold/releases/download/v$MOLD_VERSION/$TARBALL"; \
    tar -xf "$TARBALL" --strip-components 1; \
    rm "$TARBALL"

# `ARG`/`ENV` pair is a workaround for `docker build` backward-compatibility.
#
# https://github.com/docker/buildx/issues/510
ARG TARGETPLATFORM
ENV TARGETPLATFORM=${TARGETPLATFORM:-linux/amd64}

RUN xx-apt-get install -y pkg-config gcc g++ libc6-dev libunwind-dev

# Select Cargo profile (e.g., `release`, `dev` or `ci`)
ARG PROFILE=release

# Enable crate features
ARG FEATURES

# Pass custom `RUSTFLAGS` (e.g., `--cfg tokio_unstable` to enable Tokio tracing/`tokio-console`)
ARG RUSTFLAGS

# Select linker (e.g., `mold`, `lld` or an empty string for the default linker)
ARG LINKER=mold

# Select target CPU; use `rustc --print target-cpus` to see available values
ARG TARGET_CPU

# Select jemalloc's allocator page size
#
# Specified as base 2 log, e.g.:
# 12 = 2^12 = 4k
# 14 = 2^14 = 16k
# 16 = 2^16 = 64k
#
# https://github.com/tikv/jemallocator/tree/main/jemalloc-sys#environment-variables
# https://github.com/jemalloc/jemalloc/blob/dev/INSTALL.md?plain=1#L225-L232
ARG JEMALLOC_SYS_WITH_LG_PAGE

# Enable GPU support
ARG GPU

COPY --from=planner /qdrant/recipe.json recipe.json
# `PKG_CONFIG=...` is a workaround for `xx-cargo` bug for crates using `pkg-config`!
#
# https://github.com/tonistiigi/xx/issues/107
# https://github.com/tonistiigi/xx/pull/108
RUN PKG_CONFIG="/usr/bin/$(xx-info)-pkg-config" \
    PATH="$PATH:/opt/mold/bin" \
    RUSTFLAGS="${LINKER:+-C link-arg=-fuse-ld=}$LINKER ${TARGET_CPU:+-C target-cpu=}$TARGET_CPU $RUSTFLAGS" \
    ${JEMALLOC_SYS_WITH_LG_PAGE:+env JEMALLOC_SYS_WITH_LG_PAGE="${JEMALLOC_SYS_WITH_LG_PAGE}"} \
    xx-cargo chef cook --profile $PROFILE ${FEATURES:+--features} $FEATURES --features=stacktrace ${GPU:+--features=gpu} --recipe-path recipe.json

COPY . .
# Include git commit into Qdrant binary during build
ARG GIT_COMMIT_ID
# `PKG_CONFIG=...` is a workaround for `xx-cargo` bug for crates using `pkg-config`!
#
# https://github.com/tonistiigi/xx/issues/107
# https://github.com/tonistiigi/xx/pull/108
RUN PKG_CONFIG="/usr/bin/$(xx-info)-pkg-config" \
    PATH="$PATH:/opt/mold/bin" \
    RUSTFLAGS="${LINKER:+-C link-arg=-fuse-ld=}$LINKER ${TARGET_CPU:+-C target-cpu=}$TARGET_CPU $RUSTFLAGS" \
    ${JEMALLOC_SYS_WITH_LG_PAGE:+env JEMALLOC_SYS_WITH_LG_PAGE="${JEMALLOC_SYS_WITH_LG_PAGE}"} \
    xx-cargo build --profile $PROFILE ${FEATURES:+--features} $FEATURES --features=stacktrace ${GPU:+--features=gpu} --bin qdrant \
    && PROFILE_DIR=$(if [ "$PROFILE" = dev ]; then echo debug; else echo $PROFILE; fi) \
    && mv target/$(xx-cargo --print-target-triple)/$PROFILE_DIR/qdrant /qdrant/qdrant

# Download and extract web UI
RUN mkdir /static && STATIC_DIR=/static ./tools/sync-web-ui.sh

# Generate SBOM
RUN cargo sbom > qdrant.spdx.json


# Dockerfile does not support conditional `FROM` directly.
# To workaround this limitation, we use a multi-stage build with a different base images which have equal name to ARG value.

# Base image for Qdrant.
FROM debian:13-slim AS qdrant-cpu


# Base image for Qdrant with NVIDIA GPU support.
#
# GPU indexing uses Vulkan. The NVIDIA Container Toolkit mounts the driver libraries into the
# container at run time, so the image only provides the Vulkan loader and an ICD manifest.
# `vulkan-tools` provides `vulkaninfo` to check which GPUs the container sees.
#
# The manifest points to `libEGL_nvidia.so.0`. It exports the Vulkan ICD entry points like
# `libGLX_nvidia.so.0` does, but doesn't need X11 or libglvnd's `libEGL.so.1` in the image.
# The manifest has its own file name, so it stays visible when the toolkit's CDI mode mounts
# the host's `nvidia_icd.json` into `/etc/vulkan/icd.d/`.
FROM debian:13-slim AS qdrant-gpu-nvidia
RUN apt-get update \
    && apt-get install -y --no-install-recommends libvulkan1 vulkan-tools \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf/* /var/lib/dpkg/status-old
COPY --from=builder /qdrant/lib/gpu/nvidia_egl_icd.json /usr/share/vulkan/icd.d/
# Request all GPUs and the driver capabilities that Vulkan compute needs from the NVIDIA runtime.
ENV NVIDIA_VISIBLE_DEVICES=all \
    NVIDIA_DRIVER_CAPABILITIES=compute,graphics,utility


# Base image for Qdrant with AMD GPU support.
#
# GPU indexing uses Vulkan through the Mesa RADV driver. Only `/dev/dri` has to be passed into
# the container; ROCm is not needed. `vulkan-tools` provides `vulkaninfo` to check which GPUs
# the container sees.
FROM debian:13-slim AS qdrant-gpu-amd
RUN apt-get update \
    && apt-get install -y --no-install-recommends libvulkan1 mesa-vulkan-drivers vulkan-tools \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf/* /var/lib/dpkg/status-old


FROM qdrant-${GPU:+gpu-}${GPU:-cpu} AS qdrant

# Install additional packages into the container.
# E.g., the debugger of choice: gdb/gdbserver/lldb.
ARG PACKAGES

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tzdata libunwind8 $PACKAGES \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf/* /var/lib/dpkg/status-old

# Copy Qdrant source files into the container. Useful for debugging.
#
# To enable, set `SOURCES` to *any* non-empty string. E.g., 1/true/enable/whatever.
# (Note, that *any* non-empty string would work, so 0/false/disable would enable the option as well.)
ARG SOURCES

# Dockerfile does not support conditional `COPY` instructions (e.g., it's impossible to do something
# like `if [ -n "$SOURCES" ]; then COPY ...; fi`), so we *hack* conditional `COPY` by abusing
# parameter expansion and `COPY` wildcards support. 😎

ENV DIR=${SOURCES:+/qdrant/src}
COPY --from=builder ${DIR:-/null?} $DIR/

ENV DIR=${SOURCES:+/qdrant/lib}
COPY --from=builder ${DIR:-/null?} $DIR/

ENV DIR=${SOURCES:+/usr/local/cargo/registry/src}
COPY --from=builder ${DIR:-/null?} $DIR/

ENV DIR=${SOURCES:+/usr/local/cargo/git/checkouts}
COPY --from=builder ${DIR:-/null?} $DIR/

ENV DIR=""

ARG APP=/qdrant

ARG USER_ID=0

RUN if [ "$USER_ID" != 0 ]; then \
        groupadd --gid "$USER_ID" qdrant; \
        useradd --uid "$USER_ID" --gid "$USER_ID" -m qdrant; \
        mkdir -p "$APP"/storage "$APP"/snapshots; \
        chown -R "$USER_ID:$USER_ID" "$APP"; \
    fi

COPY --from=builder --chown=$USER_ID:$USER_ID /qdrant/qdrant "$APP"/qdrant
COPY --from=builder --chown=$USER_ID:$USER_ID /qdrant/qdrant.spdx.json "$APP"/qdrant.spdx.json
COPY --from=builder --chown=$USER_ID:$USER_ID /qdrant/config "$APP"/config
COPY --from=builder --chown=$USER_ID:$USER_ID /qdrant/tools/entrypoint.sh "$APP"/entrypoint.sh
COPY --from=builder --chown=$USER_ID:$USER_ID /static "$APP"/static

WORKDIR "$APP"

USER "$USER_ID:$USER_ID"

ENV TZ=Etc/UTC \
    RUN_MODE=production

EXPOSE 6333
EXPOSE 6334

LABEL org.opencontainers.image.title="Qdrant"
LABEL org.opencontainers.image.description="Official Qdrant image"
LABEL org.opencontainers.image.url="https://qdrant.com/"
LABEL org.opencontainers.image.documentation="https://qdrant.com/docs"
LABEL org.opencontainers.image.source="https://github.com/qdrant/qdrant"
LABEL org.opencontainers.image.vendor="Qdrant"

CMD ["./entrypoint.sh"]
