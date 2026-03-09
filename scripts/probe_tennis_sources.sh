#!/usr/bin/env bash
set -euo pipefail

UA='Mozilla/5.0 (compatible; WTA-Edge-Board/1.0)'
OUT_DIR="${1:-./tmp/source_probes}"
mkdir -p "$OUT_DIR"

probe() {
  local name="$1"
  local url="$2"
  local out="$OUT_DIR/$name"

  echo "==> $name"
  if curl -sS -L --max-time 30 -H "User-Agent: $UA" "$url" -o "$out"; then
    echo "saved=$out bytes=$(wc -c < "$out" | tr -d ' ')"
    sha256sum "$out" | awk '{print "sha256=" $1}'
  else
    echo "WARN: fetch failed for $url"
  fi
  echo
}

probe ta_leaders.html 'https://www.tennisabstract.com/cgi-bin/leaders_wta.cgi?players=top'
probe ta_h2h.html 'https://tennisabstract.com/reports/h2hMatrixWta.html'

echo "done"
