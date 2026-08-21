#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
UNIT_SOURCE="$PROJECT_DIR/ops/systemd"
UNIT_TARGET="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
SERVICE="metar-delta-maintenance.service"
TIMER="metar-delta-maintenance.timer"

if ! command -v systemctl >/dev/null 2>&1; then
    echo "systemctl is required to install the maintenance timer." >&2
    exit 1
fi

mkdir -p "$UNIT_TARGET"
install -m 0644 "$UNIT_SOURCE/$SERVICE" "$UNIT_TARGET/$SERVICE"
install -m 0644 "$UNIT_SOURCE/$TIMER" "$UNIT_TARGET/$TIMER"

systemctl --user daemon-reload
systemctl --user enable --now "$TIMER"
systemctl --user list-timers "$TIMER" --no-pager
