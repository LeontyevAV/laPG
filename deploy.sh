#!/bin/bash
set -e

cd /home/sasha/work/laPG

echo "=== laPG deploy $(date '+%Y-%m-%d %H:%M:%S') ==="

echo "[1/4] git pull..."
git pull

echo "[2/4] venv sync..."
./venv/bin/pip install -q -r requirements.txt

echo "[3/4] DB migrate..."
./venv/bin/python -c "from db import init_db; init_db()"

echo "[4/4] restart service..."
systemctl --user restart lapg

sleep 1
if systemctl --user is-active --quiet lapg; then
    echo "OK: laPG is running on port 8081"
else
    echo "FAIL: service not running"
    systemctl --user status lapg --no-pager
    exit 1
fi