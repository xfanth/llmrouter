# Debian, not scratch and not Alpine. A shell with curl, ss and ps answers in
# seconds what reading logs answers in minutes, and musl's malloc performs
# badly on this allocation pattern: a large JSON document parsed per client
# request across many threads.
FROM rust:slim-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl procps iproute2 \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/llmrouter /usr/local/bin/llmrouter
EXPOSE 8080
ENTRYPOINT ["llmrouter"]
