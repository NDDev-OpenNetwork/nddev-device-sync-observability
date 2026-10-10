FROM rust:1.99.0-bookworm@sha256:114c7a4425406451c2866b6aafe69fe29b1b298832db1277d411ac73c82d04d6 AS build
WORKDIR /build
ARG NDS_SOURCE_COMMIT=unknown
ENV NDS_SOURCE_COMMIT=${NDS_SOURCE_COMMIT}
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY src ./src
COPY contracts ./contracts
COPY migrations ./migrations
RUN --mount=type=cache,target=/usr/local/cargo/registry --mount=type=cache,target=/build/target \
    cargo build --locked --release -j4 && cp target/release/nddev-device-sync-observability /build/nds-observability
FROM debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /build/nds-observability /usr/local/bin/nds-observability
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/nds-observability"]
