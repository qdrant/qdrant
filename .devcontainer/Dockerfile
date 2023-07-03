FROM debian:11-slim AS qdrant

RUN apt-get update \
    && apt-get install -y ca-certificates tzdata \
    clang lld cmake protobuf-compiler jq \
    && rm -rf /var/lib/apt/lists/*

ARG APP=/qdrant

RUN mkdir -p ${APP}


WORKDIR ${APP}

ENV TZ=Etc/UTC \
    RUN_MODE=production

EXPOSE 6333
EXPOSE 6334

CMD ["./entrypoint.sh"]
