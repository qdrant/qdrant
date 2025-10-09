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

# Note: using bookworm base image to match GPU runtime image, otherwise we're
# seeing runtime errors due to libc version mismatch.
# See: <https://github.com/qdrant/qdrant/pull/7334>
FROM --platform=${BUILDPLATFORM:-linux/amd64} lukemathwalker/cargo-chef:latest-rust-1.90-bookworm  AS chef


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
    && rustup component add rustfmt

# `ARG`/`ENV` pair is a workaround for `docker build` backward-compatibility.
#
# https://github.com/docker/buildx/issues/510
ARG BUILDPLATFORM
ENV BUILDPLATFORM=${BUILDPLATFORM:-linux/amd64}

ARG MOLD_VERSION=2.36.0

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

# Target CPU; use `rustc --print target-cpus` to see available values
ARG TARGET_CPU

# Enable GPU support
ARG GPU

# jemalloc' base 2 log of page size.  For example, for the ordinary 4k page size, use 12;
# for 16k, use 14; for 64k, use 16.  It corresponds to jemalloc's `--with-lg-page` configure argument.
ARG JEMALLOC_SYS_WITH_LG_PAGE

# Download and extract web UI
COPY tools/ tools/
COPY docs/ docs/
RUN mkdir /static && STATIC_DIR=/static ./tools/sync-web-ui.sh

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

RUN xx-cargo install cargo-sbom && \
    cargo sbom > qdrant.spdx.json

# Dockerfile does not support conditional `FROM` directly.
# To workaround this limitation, we use a multi-stage build with a different base images which have equal name to ARG value.

# Base image for Qdrant.
FROM debian:13-slim AS qdrant-cpu


# Base images for Qdrant with nvidia GPU support.
FROM nvidia/opengl:1.2-glvnd-devel-ubuntu22.04 AS qdrant-gpu-nvidia
# Set non-interactive mode for apt-get.
ENV DEBIAN_FRONTEND=noninteractive
# Set NVIDIA driver capabilities. By default, all capabilities are disabled.
ENV NVIDIA_DRIVER_CAPABILITIES compute,graphics,utility
# Copy Nvidia ICD loader file into the container.
COPY --from=builder /qdrant/lib/gpu/nvidia_icd.json /etc/vulkan/icd.d/
# Override maintainer label. Nvidia base image have it's own maintainer label.
LABEL maintainer="Qdrant Team <info@qdrant.tech>"


# Base images for Qdrant with amd GPU support.
FROM rocm/dev-ubuntu-22.04 AS qdrant-gpu-amd
# Set non-interactive mode for apt-get.
ENV DEBIAN_FRONTEND=noninteractive
# Override maintainer label. AMD base image have it's own maintainer label.
LABEL maintainer="Qdrant Team <info@qdrant.tech>"


FROM qdrant-${GPU:+gpu-}${GPU:-cpu} AS qdrant

# Install GPU dependencies
ARG GPU

RUN if [ -n "$GPU" ]; then \
    apt-get update \
    && apt-get install -y \
    libvulkan1 \
    libvulkan-dev \
    vulkan-tools \
    ; fi

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
