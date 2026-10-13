#!/usr/bin/env bash
# Provision the exact Redis source/binaries used by cross-engine integration tests.
# The cache path mirrors tests/integration/_redis_path.py; no system Redis is substituted.
set -euo pipefail
redis_version=7.2.5
redis_commit=f60370ce28b946c1146dcea77c9c399d39601aaa
reference_dir="${REDIS_DIR:-${PEADB_CACHE_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/peadb}/redis-$redis_version}"
if [ -x "$reference_dir/src/redis-server" ] &&
   [ -x "$reference_dir/src/redis-cli" ] &&
   [ -x "$reference_dir/src/redis-check-rdb" ]; then
  case "$("$reference_dir/src/redis-server" --version)" in
    *" v=$redis_version "*) echo "Redis $redis_version reference ready: $reference_dir"; exit 0 ;;
  esac
fi
if [ -e "$reference_dir" ]; then
  echo "Reference cache is incomplete or has another version: $reference_dir" >&2
  exit 1
fi
scratch_dir=$(mktemp -d)
trap 'rm -rf "$scratch_dir"' EXIT
curl -fL --retry 3 "https://api.github.com/repos/redis/redis/tarball/$redis_commit" -o "$scratch_dir/redis.tar.gz"
mkdir "$scratch_dir/source"
tar -xzf "$scratch_dir/redis.tar.gz" -C "$scratch_dir/source" --strip-components=1
jobs=$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)
make -C "$scratch_dir/source/src" -j"$jobs" MALLOC=libc BUILD_TLS=no redis-server redis-cli redis-check-rdb
case "$("$scratch_dir/source/src/redis-server" --version)" in
  *" v=$redis_version "*) ;;
  *) echo 'Reference build has the wrong Redis version' >&2; exit 1 ;;
esac
mkdir -p "$(dirname "$reference_dir")"
mv "$scratch_dir/source" "$reference_dir"
echo "Redis $redis_version reference ready: $reference_dir"
