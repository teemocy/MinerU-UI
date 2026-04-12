#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Usage: $0 <target-mineru-root> [api|webui|all]" >&2
  exit 1
fi

TARGET_ROOT="$(cd "$1" && pwd)"
MODE="${2:-all}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ ! -d "$TARGET_ROOT/mineru" ]]; then
  echo "Expected a MinerU source tree at: $TARGET_ROOT" >&2
  exit 1
fi

copy_overlay() {
  local source_root="$1"
  local relative_path="$2"

  if [[ ! -d "$source_root/$relative_path" ]]; then
    return 0
  fi

  mkdir -p "$TARGET_ROOT/$relative_path"
  cp -R "$source_root/$relative_path"/. "$TARGET_ROOT/$relative_path"/
}

case "$MODE" in
  api)
    copy_overlay "$REPO_ROOT/api" "mineru"
    ;;
  webui)
    copy_overlay "$REPO_ROOT/webui" "mineru"
    ;;
  all)
    copy_overlay "$REPO_ROOT/api" "mineru"
    copy_overlay "$REPO_ROOT/webui" "mineru"
    ;;
  *)
    echo "Unsupported mode: $MODE" >&2
    exit 1
    ;;
esac

echo "Applied '$MODE' customizations into $TARGET_ROOT"
