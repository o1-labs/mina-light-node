# Build the Mina light node.
FROM rust:1-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release -p mina-light-node

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/mina-light-node /usr/local/bin/mina-light-node
ENV MINA_NETWORK=devnet
ENTRYPOINT ["mina-light-node"]
