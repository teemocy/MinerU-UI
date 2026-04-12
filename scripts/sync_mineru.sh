#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SUBMODULE_PATH="third_party/MinerU"

git -C "$REPO_ROOT" submodule sync -- "$SUBMODULE_PATH"
git -C "$REPO_ROOT" submodule update --init --remote --checkout "$SUBMODULE_PATH"

echo "MinerU submodule is now at:"
git -C "$REPO_ROOT/$SUBMODULE_PATH" rev-parse HEAD
