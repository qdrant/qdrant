FROM debian:11-slim
ARG TARGET=x86_64-unknown-linux-gnu
ARG APP=/qdrant

RUN apt-get update \
    && apt-get install -y ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

EXPOSE 6333
EXPOSE 6334

ENV TZ=Etc/UTC \
    RUN_MODE=production

RUN mkdir -p ${APP}

COPY ./target/${TARGET}/release/qdrant ${APP}/qdrant
COPY ./config ${APP}/config

WORKDIR ${APP}

CMD ["./qdrant"]
