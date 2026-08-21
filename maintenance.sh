#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${METAR_STREAM_HOME:-$HOME/metar-stream}"
LAKE_ROOT="${LAKE_PATH:-$PROJECT_DIR/lake}"
RETENTION_HOURS="${VACUUM_RETENTION_HOURS:-168}"
STATE_DIR="${XDG_STATE_HOME:-$HOME/.local/state}/metar-stream"
RUNTIME_DIR="${XDG_RUNTIME_DIR:-/tmp}"
LOCK_FILE="$RUNTIME_DIR/metar-stream-delta-maintenance.lock"
LOG_FILE="$STATE_DIR/delta-maintenance.log"

mkdir -p "$STATE_DIR"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
    echo "Delta maintenance is already running; refusing to overlap." >&2
    exit 75
fi

cd "$PROJECT_DIR"
if [[ ! -x ".venv/bin/python" ]]; then
    echo "Missing $PROJECT_DIR/.venv/bin/python; install requirements first." >&2
    exit 1
fi

{
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] starting Delta maintenance"
    .venv/bin/python delta_maintenance.py \
        --lake-path "$LAKE_ROOT" \
        --execute \
        --retention-hours "$RETENTION_HOURS" \
        "$@"
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Delta maintenance completed"
} 2>&1 | tee -a "$LOG_FILE"
