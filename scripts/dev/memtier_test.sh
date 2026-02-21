#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME="peadb"
PORT="6379"

echo "=== Stopping containers using images containing 'peadb' ==="

# Get containers whose IMAGE column contains "peadb"
CONTAINERS=$(docker ps -a --format "{{.ID}} {{.Image}}" | grep peadb | awk '{print $1}' || true)

if [ -n "${CONTAINERS:-}" ]; then
  echo "Stopping containers..."
  docker stop $CONTAINERS || true
  echo "Removing containers..."
  docker rm -f $CONTAINERS || true
else
  echo "No containers found."
fi

echo "=== Removing images containing 'peadb' ==="

IMAGES=$(docker images --format "{{.Repository}}:{{.Tag}} {{.ID}}" | grep peadb | awk '{print $2}' | sort -u || true)

if [ -n "${IMAGES:-}" ]; then
  docker rmi -f $IMAGES
else
  echo "No images found."
fi

echo "=== Building new image '${IMAGE_NAME}:latest' ==="
docker build -t ${IMAGE_NAME}:latest .

echo "=== Running ${IMAGE_NAME} container ==="
docker run -d --name ${IMAGE_NAME}-server -p ${PORT}:6379 ${IMAGE_NAME}:latest

echo "Waiting 3 seconds for server to start..."
sleep 3

echo "=== Running memtier benchmark ==="
docker run --rm --network host redislabs/memtier_benchmark \
  --server=127.0.0.1 \
  --port=${PORT} \
  --threads=8 \
  --clients=200 \
  --ratio=1:1 \
  --data-size=256 \
  --test-time=30

echo "=== Done ==="
