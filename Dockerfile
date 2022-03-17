FROM rust:1.59 as builder

COPY . ./qdrant
WORKDIR ./qdrant

RUN apt-get update ; apt-get install -y clang

# Build actual target here
RUN cargo build --release --bin qdrant

FROM debian:bullseye-slim
ARG APP=/qdrant

RUN apt-get update \
    && apt-get install -y ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

EXPOSE 6333
EXPOSE 6334

ENV TZ=Etc/UTC \
    RUN_MODE=production

RUN mkdir -p ${APP}

COPY --from=builder /qdrant/target/release/qdrant ${APP}/qdrant
COPY --from=builder /qdrant/config ${APP}/config

WORKDIR ${APP}

CMD ["./qdrant"]
