#!/usr/bin/env bash
set -uo pipefail

PROJECT_DIR="$HOME/metar-stream"
SESSION="metar"
VENV="$PROJECT_DIR/.venv/bin/activate"

cd "$PROJECT_DIR" || { echo "no $PROJECT_DIR"; exit 1; }

# pgrep -c prints "0" and exits non-zero when nothing matches, so a plain
# `|| echo 0` fallback appends a second zero. Normalise here instead.
count_procs() {
    local n
    n=$(pgrep -fc "$1" 2>/dev/null)
    echo "${n:-0}"
}

status() {
    echo "--- docker daemon ---"
    if docker info >/dev/null 2>&1; then echo "  running"; else echo "  DOWN"; fi
    echo "--- containers ---"
    docker compose ps 2>/dev/null | tail -n +2 || echo "  none"
    echo "--- processes ---"
    echo "  spark job:  $(count_procs metar_stream)   (must be 0 or 1)"
    echo "  producer:   $(count_procs metar_producer)"
    echo "--- tmux ---"
    tmux ls 2>/dev/null || echo "  no sessions"
}

stop_procs() {
    echo ">> stopping python processes"
    tmux kill-session -t "$SESSION" 2>/dev/null
    pkill -f metar_producer 2>/dev/null
    pkill -f metar_stream 2>/dev/null
    sleep 3
    echo ">> spark=$(count_procs metar_stream) producer=$(count_procs metar_producer)"
}

case "${1:-start}" in
    status) status; exit 0 ;;
    stop)   stop_procs; exit 0 ;;
esac

if ! docker info >/dev/null 2>&1; then
    echo ">> starting docker"
    sudo systemctl start docker 2>/dev/null || sudo service docker start
    for _ in $(seq 1 30); do docker info >/dev/null 2>&1 && break; sleep 1; done
fi
docker info >/dev/null 2>&1 || { echo "docker failed to start"; exit 1; }

echo ">> bringing up containers"
docker compose up -d
echo ">> waiting for redpanda"
for _ in $(seq 1 60); do
    docker compose ps redpanda 2>/dev/null | grep -q healthy && { echo "   healthy"; break; }
    sleep 2
done

SPARK_RUNNING=$(count_procs metar_stream)
PROD_RUNNING=$(count_procs metar_producer)
[ "$SPARK_RUNNING" -gt 0 ] && echo ">> spark already running — leaving it alone"
[ "$PROD_RUNNING" -gt 0 ] && echo ">> producer already running — leaving it alone"

if ! tmux has-session -t "$SESSION" 2>/dev/null; then
    tmux new-session -d -s "$SESSION" -n producer
    tmux new-window -t "$SESSION" -n spark
fi

if [ "$PROD_RUNNING" -eq 0 ]; then
    echo ">> starting producer"
    tmux send-keys -t "$SESSION:producer" "cd $PROJECT_DIR && source $VENV && python metar_producer.py" C-m
fi

if [ "$SPARK_RUNNING" -eq 0 ]; then
    echo ">> starting spark job"
    tmux send-keys -t "$SESSION:spark" "cd $PROJECT_DIR && source $VENV && python metar_stream.py" C-m
fi

sleep 5
echo
status
echo
echo "attach:  tmux attach -t $SESSION     (Ctrl-B then D to leave it running)"
