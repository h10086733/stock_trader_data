#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

python3 fetch.py --sync
python3 index_stats.py --calc-intraday --index-codes 000300,000852

for attempt in 1 2 3; do
  if python3 -m trader.cli --csi1000-daily --csi1000-sync-index --csi1000-lookback-days 180; then
    exit 0
  fi
  echo "csi1000 timing refresh failed, retry ${attempt}/3" >&2
  sleep $((attempt * 10))
done

python3 -m trader.cli --csi1000-daily --csi1000-sync-index --csi1000-lookback-days 180
