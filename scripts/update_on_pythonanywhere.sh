#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/home/valjuluchurch/church_finance"

if [ ! -d "$APP_DIR/.git" ]; then
  echo "ERROR: $APP_DIR is not a git repository"
  exit 1
fi

cd "$APP_DIR"

echo "=== [1/5] Current branch ==="
git branch --show-current

echo "=== [2/5] Fetch latest ==="
git fetch origin

echo "=== [3/5] Pull latest (fast-forward only) ==="
git pull --ff-only

echo "=== [4/5] Syntax check ==="
python3 -m py_compile app.py

echo "=== [5/5] Done ==="
echo "Update complete. Please click Reload in PythonAnywhere Web tab."
