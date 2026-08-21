#!/usr/bin/env bash
# Export the gold snapshot and publish it to the `data` branch.
#
# Uses a git worktree at ../metar-data so the data branch is checked out in a
# separate directory. This script never switches branches in the main tree and
# never touches your working files.
set -uo pipefail

PROJECT_DIR="$HOME/metar-stream"
DATA_DIR="$HOME/metar-data"
LOG="$PROJECT_DIR/publish.log"

cd "$PROJECT_DIR" || exit 1

# Only one publish at a time. Concurrent Spark sessions contend over the Ivy
# cache and produce "unknown resolver null" resolution failures.
exec 9>"$PROJECT_DIR/.publish.lock"
flock -n 9 || exit 0

log() { echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] $*" >> "$LOG"; }
log "--- publish start ---"

if ! docker info >/dev/null 2>&1; then log "docker down, skipping"; exit 0; fi
if [ "$(pgrep -fc metar_stream 2>/dev/null || echo 0)" -eq 0 ]; then log "spark not running, skipping"; exit 0; fi
[ -d "$DATA_DIR" ] || { log "worktree missing at $DATA_DIR"; exit 1; }

source "$PROJECT_DIR/.venv/bin/activate" || { log "venv missing"; exit 1; }
if ! python export_dashboard.py >> "$LOG" 2>&1; then log "export failed"; exit 1; fi
[ -f docs/metar_pipeline.json ] || { log "no output"; exit 1; }

mkdir -p "$DATA_DIR/docs"
cp docs/metar_pipeline.json "$DATA_DIR/docs/metar_pipeline.json"

cd "$DATA_DIR" || { log "cannot enter worktree"; exit 1; }

# GitHub Actions also commits to this branch hourly, so pull before pushing.
git fetch origin data >> "$LOG" 2>&1
git rebase origin/data >> "$LOG" 2>&1 || { log "rebase failed"; git rebase --abort; exit 1; }

git add docs/metar_pipeline.json >> "$LOG" 2>&1
if git diff --cached --quiet; then
    log "no change in snapshot"
else
    git commit -m "snapshot $(date -u +'%Y-%m-%d %H:%M UTC')" >> "$LOG" 2>&1
    git push origin data >> "$LOG" 2>&1 && log "pushed" || log "PUSH FAILED"
fi
log "--- publish done ---"
