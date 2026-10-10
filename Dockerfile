# syntax=docker/dockerfile:1
FROM rust:1.98-slim-bookworm AS builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates stockfish \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/stockfish-relay /usr/local/bin/stockfish-relay
COPY deploy/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod 0755 /usr/local/bin/docker-entrypoint.sh
USER 10001:10001
ENV STOCKFISH_RELAY_MODE=master \
    STOCKFISH_RELAY_LISTEN=0.0.0.0:8080 \
    STOCKFISH_PATH=/usr/games/stockfish
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
