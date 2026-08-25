#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
if [[ $# -ne 1 ]]; then
  echo "Usage: $0 YYYY-MM-DD" >&2
  exit 2
fi
BIZ_DATE="$1"
DATE_MINUS_6="$(date -d "$BIZ_DATE -6 day" +%F)"
DATE_MINUS_29="$(date -d "$BIZ_DATE -29 day" +%F)"
tmp_file="$(mktemp)"
trap 'rm -f -- "$tmp_file"' EXIT
sed -e "s/__BIZ_DATE__/$BIZ_DATE/g" \
  -e "s/__DATE_MINUS_6__/$DATE_MINUS_6/g" \
  -e "s/__DATE_MINUS_29__/$DATE_MINUS_29/g" \
  flink/sql/11_initialize_dm.sql > "$tmp_file"
docker compose cp "$tmp_file" flink-jobmanager:/tmp/initialize-dm.sql
batch_output="$(docker compose exec -T flink-jobmanager /opt/flink/bin/sql-client.sh -f /tmp/initialize-dm.sql 2>&1)"
printf '%s\n' "$batch_output"
if grep -q '\[ERROR\]' <<<"$batch_output"; then
  echo "Paimon DM initialization failed" >&2
  exit 1
fi
echo "DM full snapshot base partition $BIZ_DATE initialized."
