#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/home/valjuluchurch/church_finance"
ERROR_LOG="/var/log/valjuluchurch.pythonanywhere.com.error.log"
SERVER_LOG="/var/log/valjuluchurch.pythonanywhere.com.server.log"

if [ ! -d "$APP_DIR" ]; then
  echo "ERROR: app directory not found: $APP_DIR"
  exit 1
fi

cd "$APP_DIR"

echo "=== [1/7] Basic info ==="
date
pwd

echo "=== [2/7] Git info ==="
if [ -d .git ]; then
  git branch --show-current || true
  git log -1 --pretty=format:'%h %ad %s' --date=iso || true
  git status --short || true
else
  echo "WARN: not a git repository"
fi

echo "=== [3/7] Required files ==="
for f in app.py models.py church.db; do
  if [ -f "$f" ]; then
    ls -lh "$f"
  else
    echo "ERROR: missing file: $APP_DIR/$f"
    exit 1
  fi
done

echo "=== [4/7] Python syntax check ==="
python3 -m py_compile app.py

echo "=== [5/7] Error log (latest 20 lines) ==="
if [ -f "$ERROR_LOG" ]; then
  tail -n 20 "$ERROR_LOG"
else
  echo "WARN: missing error log: $ERROR_LOG"
fi

echo "=== [6/7] Server log (latest 20 lines) ==="
if [ -f "$SERVER_LOG" ]; then
  tail -n 20 "$SERVER_LOG"
else
  echo "WARN: missing server log: $SERVER_LOG"
fi

echo "=== [7/7] Done ==="
echo "If update was just deployed, click Reload in PythonAnywhere Web tab first."
