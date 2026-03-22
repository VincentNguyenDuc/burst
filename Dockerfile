FROM rust:1-bookworm AS builder
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY burst-core ./burst-core
COPY burst-controller ./burst-controller
COPY burst-worker ./burst-worker
COPY burst-cli ./burst-cli
COPY burst-config ./burst-config
COPY scripts ./scripts

RUN cargo build --workspace --release

FROM debian:bookworm-slim AS runtime
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates python3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/burst-controller /usr/local/bin/burst-controller
COPY --from=builder /app/target/release/burst-worker /usr/local/bin/burst-worker
COPY --from=builder /app/target/release/burst-cli /usr/local/bin/burst-cli
COPY --from=builder /app/scripts /app/scripts
COPY --from=builder /app/burst-config /app/burst-config

RUN chmod +x /app/scripts/*.sh
RUN chmod +x /app/scripts/*.sh

CMD ["/usr/local/bin/burst-controller", "--config", "/app/burst-config/burst.config.json"]
