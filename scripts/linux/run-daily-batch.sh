#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
BIZ_DATE="${1:-$(date -d yesterday +%F)}"
PREV_DATE="$(date -d "$BIZ_DATE -1 day" +%F)"
DATE_MINUS_7="$(date -d "$BIZ_DATE -7 day" +%F)"
DATE_MINUS_30="$(date -d "$BIZ_DATE -30 day" +%F)"
BIZ_DATE_COMPACT="$(date -d "$BIZ_DATE" +%Y%m%d)"
tmp_file="$(mktemp)"
trap 'rm -f -- "$tmp_file"' EXIT
{
  cat flink/sql/04_daily_dws.sql
  printf '\n'
  cat flink/sql/10_daily_offline.sql
} | sed -e "s/__BIZ_DATE_COMPACT__/$BIZ_DATE_COMPACT/g" \
  -e "s/__BIZ_DATE__/$BIZ_DATE/g" \
  -e "s/__PREV_DATE__/$PREV_DATE/g" \
  -e "s/__DATE_MINUS_7__/$DATE_MINUS_7/g" \
  -e "s/__DATE_MINUS_30__/$DATE_MINUS_30/g" > "$tmp_file"
docker compose cp "$tmp_file" flink-jobmanager:/tmp/run-daily-offline.sql
batch_output="$(docker compose exec -T flink-jobmanager /opt/flink/bin/sql-client.sh -f /tmp/run-daily-offline.sql 2>&1)"
printf '%s\n' "$batch_output"
if grep -q '\[ERROR\]' <<<"$batch_output"; then
  echo "Paimon offline batch failed" >&2
  exit 1
fi
echo "Offline partition $BIZ_DATE is available through the StarRocks Paimon catalog."
