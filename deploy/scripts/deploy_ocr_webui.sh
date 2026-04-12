#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

IMAGE_TAG="${IMAGE_TAG:-mineru-ocr-webui:local}"
CONTAINER_NAME="${CONTAINER_NAME:-mineru-ocr-webui}"
WEBUI_PORT="${WEBUI_PORT:-7861}"
API_URL="${API_URL:-http://host.docker.internal:8000}"
OUTPUT_ROOT="${OUTPUT_ROOT:-$REPO_ROOT/output/ocr-webui}"
INPUT_ROOT="${INPUT_ROOT:-}"
DOCKERFILE="${DOCKERFILE:-deploy/docker/Dockerfile.mineru-ocr-webui}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required but was not found in PATH" >&2
  exit 1
fi

mkdir -p "$OUTPUT_ROOT"

if docker ps -a --format '{{.Names}}' | grep -Fxq "$CONTAINER_NAME"; then
  docker rm -f "$CONTAINER_NAME" >/dev/null
fi

docker build -t "$IMAGE_TAG" -f "$REPO_ROOT/$DOCKERFILE" "$REPO_ROOT"

docker_args=(
  run
  -d
  --name "$CONTAINER_NAME"
  --restart unless-stopped
  --add-host host.docker.internal:host-gateway
  -p "${WEBUI_PORT}:7861"
  -v "${OUTPUT_ROOT}:/data/ocr-webui"
)

if [[ -n "$INPUT_ROOT" ]]; then
  docker_args+=(-v "${INPUT_ROOT}:/data/input:ro")
fi

docker_args+=(
  "$IMAGE_TAG"
  --host 0.0.0.0
  --port 7861
  --api-url "$API_URL"
  --output-root /data/ocr-webui
)

docker "${docker_args[@]}"

echo "Container: $CONTAINER_NAME"
echo "WebUI: http://127.0.0.1:${WEBUI_PORT}"
echo "MinerU API: $API_URL"
echo "Output root: $OUTPUT_ROOT"
if [[ -n "$INPUT_ROOT" ]]; then
  echo "Mounted input root: $INPUT_ROOT -> /data/input"
fi
