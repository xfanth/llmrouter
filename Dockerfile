# Debian, not scratch and not Alpine. A shell with curl, ss and ps answers in
# seconds what reading logs answers in minutes, and musl's malloc performs
# badly on this allocation pattern: a large JSON document parsed per client
# request across many threads.
#
# cargo-chef splits the build so the dependencies get their own cached layer.
# The planner reduces the manifest to recipe.json, which changes only when the
# dependency set does, so a source-only change skips straight to compiling the
# crate. The image is rust:slim-bookworm with cargo-chef on top: the same
# toolchain as before, so the binary that ships is built the same way. The Rust
# version in the tag matches rust-toolchain.toml, which .dockerignore keeps out
# of the build context, so bump the two together.
FROM lukemathwalker/cargo-chef:0.1.78-rust-1.99.0-slim-bookworm AS chef

# tikv-jemalloc-sys compiles jemalloc from C source, so the build needs a C
# toolchain. The base image has cc but no make, which is why
# `cargo build --release` exited 101 here before. Installed in the shared base
# because the cook stage is where jemalloc actually compiles.
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS build
COPY --from=planner /src/recipe.json recipe.json
# Same release profile as the final build, so the cooked dependencies are
# reused as-is rather than rebuilt. Only the llmrouter bin ships, so only it is
# built: llmrouter-make-config would otherwise get its own slow LTO link here
# and then be thrown away. --bin on the cook keeps the cooked set matching.
# That leaves make-config with no release build here, so the gate builds it
# itself with `cargo build --release --bin llmrouter-make-config`: see
# job_clippy in ci.sh and the matching step in .github/workflows/ci.yml.
RUN cargo chef cook --release --bin llmrouter --recipe-path recipe.json
COPY . .
# The publish action passes both as build args. Declared after the cook so a
# new commit does not invalidate the cooked dependencies. `GET /version`
# reports them.
ARG GITHUB_SHA=unknown
ARG BUILD_DATE=unknown
RUN LLMROUTER_GIT_SHA="$GITHUB_SHA" LLMROUTER_BUILD_DATE="$BUILD_DATE" \
    cargo build --release --bin llmrouter

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl procps iproute2 \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/llmrouter /usr/local/bin/llmrouter
EXPOSE 8080
ENTRYPOINT ["llmrouter"]
