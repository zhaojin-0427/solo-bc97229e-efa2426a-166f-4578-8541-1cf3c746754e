#!/usr/bin/env bash
# 本地（无 Docker）启动脚本
set -euo pipefail
cd "$(dirname "$0")"
export APP_DATA_DIR="${APP_DATA_DIR:-$(pwd)/data}"
export APP_DB_PATH="${APP_DATA_DIR}/patients.db"
export APP_UPLOAD_DIR="${APP_DATA_DIR}/uploads"
mkdir -p "$APP_UPLOAD_DIR"
exec gunicorn --bind 0.0.0.0:8000 --workers 1 --threads 8 --timeout 120 app:app
