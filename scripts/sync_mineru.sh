#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SUBMODULE_PATH="third_party/MinerU"
SUBMODULE_ROOT="$REPO_ROOT/$SUBMODULE_PATH"
REMOTE_NAME="${MINERU_REMOTE_NAME:-origin}"
TAG_PATTERN="${MINERU_TAG_PATTERN:-mineru-*}"

git -C "$REPO_ROOT" submodule sync -- "$SUBMODULE_PATH"
git -C "$REPO_ROOT" submodule update --init --checkout "$SUBMODULE_PATH"
git -C "$SUBMODULE_ROOT" fetch "$REMOTE_NAME" --tags --force --prune

LATEST_TAG="$(
  git -C "$SUBMODULE_ROOT" tag -l "$TAG_PATTERN" --sort=-version:refname | head -n 1
)"

if [[ -z "$LATEST_TAG" ]]; then
  echo "No tags matched pattern '$TAG_PATTERN' in $SUBMODULE_PATH" >&2
  exit 1
fi

git -C "$SUBMODULE_ROOT" checkout --detach "$LATEST_TAG"

echo "MinerU submodule is now at tag: $LATEST_TAG"
git -C "$SUBMODULE_ROOT" rev-parse HEAD
