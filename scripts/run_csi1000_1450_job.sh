#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

python3 fetch.py --sync
python3 index_stats.py --calc-intraday --index-codes 000300,000852
python3 app.py --csi1000-daily --csi1000-sync-index
