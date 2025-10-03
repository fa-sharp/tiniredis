ARG RUST_VERSION=1.85
ARG DEBIAN_VERSION=bookworm

### Build Rust backend ###
FROM rust:${RUST_VERSION}-slim-${DEBIAN_VERSION} AS build
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY client ./client

ARG pkg=tinikeyval

RUN --mount=type=cache,id=rust_target,target=/app/target \
    --mount=type=cache,id=cargo_registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=cargo_git,target=/usr/local/cargo/git \
    set -eux; \
    cargo build --package $pkg --release; \
    objcopy --compress-debug-sections target/release/$pkg ./run-server


### Final image ###
FROM debian:${DEBIAN_VERSION}-slim

# Create non-root user
ARG UID=10001
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/home/appuser" \
    --shell "/sbin/nologin" \
    --uid "${UID}" \
    appuser

USER appuser

# Copy app files
COPY --from=build /app/run-server /usr/local/bin/

# Run
ENV HOST=0.0.0.0
CMD ["run-server"]
