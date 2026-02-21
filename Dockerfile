# ── Build stage ──────────────────────────────────────────────────────────────
FROM ubuntu:24.04 AS builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential cmake g++ git ca-certificates \
        liblua5.1-0-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

RUN cmake -S . -B build \
        -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_TESTING=OFF && \
    cmake --build build --target peadb-server -j"$(nproc)"

# ── Runtime stage ────────────────────────────────────────────────────────────
FROM ubuntu:24.04 AS runtime

RUN apt-get update && \
    apt-get install -y --no-install-recommends libstdc++6 liblua5.1-0 && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd -r peadb && useradd -r -g peadb peadb && \
    mkdir -p /data && chown peadb:peadb /data

COPY --from=builder /src/build/peadb-server /usr/local/bin/peadb-server

# Persistence volume
VOLUME /data
WORKDIR /data

EXPOSE 6379

USER peadb

ENTRYPOINT ["peadb-server"]
CMD ["--port", "6379", "--bind", "0.0.0.0", "--dir", "/data"]
