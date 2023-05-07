FROM rust:latest as builder

# based on https://github.com/docker/buildx/issues/510
ARG TARGETARCH
ENV TARGETARCH=${TARGETARCH:-amd64}

WORKDIR /qdrant

COPY ./tools/target_arch.sh ./target_arch.sh
RUN echo "Building for $TARGETARCH, arch: $(bash target_arch.sh)"

RUN apt-get update \
    && ( apt-get install -y gcc-multilib || echo "Warning: not installing gcc-multilib" ) \
    && apt-get install -y clang cmake gcc-aarch64-linux-gnu g++-aarch64-linux-gnu protobuf-compiler \
    && rustup component add rustfmt

RUN rustup target add $(bash target_arch.sh)

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
COPY --from=builder /qdrant/tools/entrypoint.sh ${APP}/entrypoint.sh

WORKDIR ${APP}

CMD ["./entrypoint.sh"]
