#!/usr/bin/env bash
#
# install_deps_ubuntu.sh — Install Ubuntu/Debian packages for building and testing PeaDB.
#
# Purpose
#   Automates the one-time package installation so that a fresh Ubuntu/Debian
#   machine can build and test PeaDB from source.
#
# What it does
#   1. Runs ``apt-get update`` to refresh the package index.
#   2. Installs core build dependencies:
#        build-essential, cmake, pkg-config
#   3. Installs Lua 5.1 development headers (``liblua5.1-0-dev``), which are
#      required by PeaDB's embedded Lua scripting engine.
#   4. Installs test-harness prerequisites:
#        tcl   — needed by the upstream Redis Tcl test suite
#        curl  — needed to download Redis source tarballs
#   5. Optionally installs ``redis-tools`` (provides ``redis-cli``) for local
#      smoke testing.  This step uses ``|| true`` so the script does not fail
#      if the package is unavailable.
#
# How to run
#   From the repo root (requires sudo):
#
#       scripts/dev/install_deps_ubuntu.sh
#
#   No arguments or environment variables are required.
#
# Prerequisites
#   - Ubuntu or Debian (or a derivative with ``apt-get``).
#   - ``sudo`` access to install system packages.
#
# Interpreting the output
#   Standard apt-get progress output.  On success the script prints:
#       Done. Try: cmake -S . -B build && cmake --build build -j
#
# Exit codes
#   0   All packages installed successfully.
#   1   The system does not have ``apt-get`` (not Ubuntu/Debian).
#   Non-zero   ``apt-get`` failed (network error, missing repo, etc.).
set -euo pipefail

if ! command -v apt-get >/dev/null 2>&1; then
  echo "apt-get not found; this script is for Ubuntu/Debian." >&2
  exit 1
fi

sudo apt-get update

# Core build deps + Lua 5.1 (required by peadb_core)
sudo apt-get install -y \
  build-essential \
  cmake \
  pkg-config \
  liblua5.1-0-dev

# Test harness deps (Redis TCL suite fetch/build + running scripts)
sudo apt-get install -y \
  tcl \
  curl

# Optional: convenient CLI for local smoke testing
sudo apt-get install -y \
  redis-tools || true

echo "Done. Try: cmake -S . -B build && cmake --build build -j"