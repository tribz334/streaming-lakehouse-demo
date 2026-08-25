#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
output="$(docker compose exec -T flink-jobmanager /opt/flink/bin/sql-client.sh \
  -f "/opt/flink/usrlib/sql/00_bootstrap.sql" 2>&1)"
printf '%s\n' "$output"
! grep -q '\[ERROR\]' <<<"$output"
docker compose exec -T -u 0 flink-jobmanager chown -R flink:flink /warehouse
echo "Fluss hot tables and native Paimon offline tables are ready."
