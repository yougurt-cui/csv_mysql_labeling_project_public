#!/usr/bin/env bash
# 快速运行 product_entities 标签任务的封装。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
python3 "$SCRIPT_DIR/tag_product_entities.py" --force "$@"
