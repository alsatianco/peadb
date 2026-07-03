# ── Build stage ──────────────────────────────────────────────────────────────
FROM ubuntu:24.04 AS builder

# Lua 5.1 is bundled in-tree (third_party/lua-5.1.5) — no Lua packages needed.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential cmake g++ git ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

# CMake places the binary at the source root (RUNTIME_OUTPUT_DIRECTORY), not build/.
RUN cmake -S . -B build \
        -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_TESTING=OFF && \
    cmake --build build --target peadb-server -j"$(nproc)"

# ── Runtime stage ────────────────────────────────────────────────────────────
FROM ubuntu:24.04 AS runtime

RUN apt-get update && \
    apt-get install -y --no-install-recommends libstdc++6 && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd -r peadb && useradd -r -g peadb peadb && \
    mkdir -p /data && chown peadb:peadb /data

COPY --from=builder /src/peadb-server /usr/local/bin/peadb-server

# Persistence volume
VOLUME /data
WORKDIR /data

EXPOSE 6379

USER peadb

ENTRYPOINT ["peadb-server"]
CMD ["--port", "6379", "--bind", "0.0.0.0", "--dir", "/data"]
