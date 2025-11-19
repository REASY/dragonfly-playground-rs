FROM rust:1.91.1

WORKDIR /app

ARG COMMIT_SHA
ARG BUILD_DATE
ARG VERSION

# System deps to compile Rust crates that may need native deps
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends cmake libsasl2-dev libclang-dev \
    iputils-ping net-tools curl binutils python3 pkg-config \
          libdw-dev libssl-dev libsasl2-dev git unzip linux-perf && \
    rm -rf /var/lib/apt/lists/*

# Copy code
COPY . .

ENTRYPOINT ["/bin/bash"]

LABEL \
    org.opencontainers.image.name="dragonfly-playground-rs" \
    org.opencontainers.image.description="A playground app to talk to DragonflyDB from Rust" \
    org.opencontainers.image.url="https://github.com/REASY/dragonfly-playground-rs" \
    org.opencontainers.image.source="https://github.com/REASY/dragonfly-playground-rs" \
    org.opencontainers.image.version="$VERSION" \
    org.opencontainers.image.licenses="MIT License" \
    org.opencontainers.image.authors="Artavazd Balaian <reasyu@gmail.com>" \
    org.opencontainers.image.base.name="rust:1.91.1" \
    org.opencontainers.image.created="$BUILD_DATE" \
    org.opencontainers.image.revision="${COMMIT_SHA}"
