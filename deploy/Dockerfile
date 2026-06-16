# Build the Mina light-node HTTP server (mina-light-node-server).
#
# The image's deps are pinned by the branch it's built from:
#   - default/devnet:  workspace pins o1-labs/mina-rust@ab69eaed + proof-systems 0.3.0
#   - mesa-mut:        the feat/mesa-mut branch pins dkijania/mina-rust@49cc4ea3 +
#                      proof-systems ab84160 (a single binary can't verify both).
# So build the mesa image from the feat/mesa-mut checkout.
FROM rust:1-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release --bin mina-light-node-server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/mina-light-node-server /usr/local/bin/mina-light-node-server

# Network + bind address (override per network). For mesa-mut, also set MINA_VK_JSON to a
# mounted verification-key JSON (no embedded VK for hardfork networks).
ENV MINA_NETWORK=devnet \
    LIGHT_NODE_HTTP_ADDR=0.0.0.0:8645 \
    RUST_LOG=mina_light_node=info
EXPOSE 8645
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=5 \
    CMD curl -fsS http://127.0.0.1:8645/health || exit 1
ENTRYPOINT ["mina-light-node-server"]
