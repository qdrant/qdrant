# Leveraging the pre-built Docker images with
# cargo-chef and the Rust toolchain
# https://www.lpalmieri.com/posts/fast-rust-docker-builds/
FROM lukemathwalker/cargo-chef:latest-rust-1.63.0 AS chef
WORKDIR /qdrant

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef as builder

ARG TARGETARCH=arm64

WORKDIR /qdrant

COPY --from=planner /qdrant/recipe.json recipe.json

RUN apt-get update
RUN env ACCEPT_EULA=Y apt upgrade
RUN apt-get install -y clang cmake
RUN rustup component add rustfmt



RUN apt-get install -y g++-multilib
RUN apt-get install -y gcc-aarch64-linux-gnu g++-aarch64-linux-gnu
RUN apt-get install -y binutils binutils-aarch64-linux-gnu

#RUN apt-get update && apt-get install -y \
#    g++-aarch64-linux-gnu \
#    libc6-dev-arm64-cross \
#    binutils \
#    binutils-aarch64-linux-gnu \
    # gcc-multilib \
#    gcc-multilib \
#    gcc-aarch64-linux-gnu \
#    g++-aarch64-linux-gnu

# COPY tools/deny-debian-packages.sh /
# RUN TARGET_ARCH=arm64 /deny-debian-packages.sh \
#    binutils \
#    binutils-aarch64-linux-gnu



RUN dpkg -S libc-header-start.h
COPY ./tools/target_arch.sh ./target_arch.sh
RUN rustup target add $(bash target_arch.sh)

# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --target $(bash target_arch.sh) --recipe-path recipe.json

COPY . .

# Build actual target here
RUN cargo build --release --target $(bash target_arch.sh) --bin qdrant

RUN mv target/$(bash target_arch.sh)/release/qdrant /qdrant/qdrant

FROM debian:11-slim
ARG APP=/qdrant

RUN apt-get update \
    && apt-get install -y ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

EXPOSE 6333
EXPOSE 6334

ENV TZ=Etc/UTC \
    RUN_MODE=production

RUN mkdir -p ${APP}

COPY --from=builder /qdrant/qdrant ${APP}/qdrant
COPY --from=builder /qdrant/config ${APP}/config

WORKDIR ${APP}

CMD ["./qdrant"]
