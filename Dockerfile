FROM rust:1.51 as builder

COPY . ./qdrant
WORKDIR ./qdrant

ENV OPENBLAS_TARGET=CORE2
RUN apt-get update ; apt-get install -y clang libopenblas-dev libgfortran-8-dev gfortran

# Build actual target here
RUN cargo build --release --bin qdrant

FROM debian:buster-slim
ARG APP=/qdrant

RUN apt-get update \
    && apt-get install -y ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

EXPOSE 6333

ENV TZ=Etc/UTC \
    RUN_MODE=production \
    OPENBLAS_NUM_THREADS=1

RUN mkdir -p ${APP}

COPY --from=builder /qdrant/target/release/qdrant ${APP}/qdrant
COPY --from=builder /qdrant/config ${APP}/config

WORKDIR ${APP}

CMD ["./qdrant"]
