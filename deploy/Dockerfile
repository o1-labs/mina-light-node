# Build the Mina light node (mina-relay + the mina-verify trust gate).
#
# It compiles mina-verify's recursive-proof verifier (proof-systems / kimchi), which
# needs a C toolchain + openssl at build time and libssl at runtime.

FROM rust:1-bookworm AS build
# rust:1-bookworm (buildpack-deps based) already ships these, but be explicit so the
# build can't silently depend on the base image's contents.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential pkg-config libssl-dev git ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p mina-light-node
RUN strip target/release/mina-light-node target/release/mina-light-node-server

FROM debian:bookworm-slim
# libssl3: the binary links openssl dynamically (openssl-sys in the dep tree).
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/mina-light-node /usr/local/bin/mina-light-node
COPY --from=build /src/target/release/mina-light-node-server /usr/local/bin/mina-light-node-server
# The network is baked at BUILD time, so the container is truly configureless —
# `docker run` needs no env, no flags. One image == one network:
#   docker build -t mina-light-node:devnet  .                               # devnet (default)
#   docker build -t mina-light-node:mainnet --build-arg MINA_NETWORK=mainnet .
#   docker run --rm -p 8645:8645 mina-light-node:devnet   # full node + trustless HTTP API
#
# Default entrypoint is the HTTP server (node + trustless read/submit/mempool API). For
# the headless node only (no HTTP, no account-index sweep):
#   docker run --rm --entrypoint mina-light-node mina-light-node:devnet
ARG MINA_NETWORK=devnet
ENV MINA_NETWORK=${MINA_NETWORK}
ENV RUST_LOG=info
# Bind the API on all interfaces inside the container (default is loopback only).
ENV LIGHT_NODE_HTTP_ADDR=0.0.0.0:8645
EXPOSE 8645
ENTRYPOINT ["mina-light-node-server"]
