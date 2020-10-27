FROM rust:1.47 as builder

COPY . ./qdrant
WORKDIR ./qdrant

# Build actual target here
RUN cargo build --release --bin qdrant

FROM debian:buster-slim
ARG APP=/qdrant

RUN apt-get update \
    && apt-get install -y ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

EXPOSE 6333

ENV TZ=Etc/UTC \
    APP_USER=qdrant \
    RUN_MODE=production

RUN groupadd $APP_USER \
    && useradd -g $APP_USER $APP_USER \
    && mkdir -p ${APP}

COPY --from=builder /qdrant/target/release/qdrant ${APP}/qdrant
COPY --from=builder /qdrant/config ${APP}/config

RUN chown -R $APP_USER:$APP_USER ${APP}

USER $APP_USER
WORKDIR ${APP}

CMD ["./qdrant"]
