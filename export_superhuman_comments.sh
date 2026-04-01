#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ "${1:-}" == "--page-indexeddb" ]]; then
  shift
  exec bash "$SCRIPT_DIR/export_superhuman_comments_page_indexeddb.sh" "$@"
fi

exec python3 "$SCRIPT_DIR/export_superhuman_comments_sqlite.py" "$@"
