#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "=== Pulling latest from origin/main ==="
git pull origin main

echo ""
echo "=== Restarting quant_dashboard service ==="
sudo systemctl restart quant_dashboard

echo ""
echo "=== Done ==="
