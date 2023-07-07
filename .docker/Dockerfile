
ARG RUST_VERSION

FROM rust:${RUST_VERSION:-1}-bullseye

RUN apt-get update && \
    apt-get install -y ca-certificates tzdata \
    clang lld cmake protobuf-compiler jq 

RUN rustup toolchain install nightly && \
    rustup component add rustfmt --toolchain nightly

COPY . .
# ARG APP=/qdrant

# RUN mkdir -p ${APP}

# WORKDIR ${APP}

EXPOSE 6333
EXPOSE 6334