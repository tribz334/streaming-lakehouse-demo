#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

for job in 02_realtime_ods.sql 03_realtime_dwd.sql 04_realtime_dws_metrics.sql 05_realtime_starrocks_relay.sql; do
  log="/tmp/${job}.log"
  run_file="/tmp/run_${job}"
  docker compose exec -d flink-jobmanager /bin/bash -lc \
    "cat /opt/flink/usrlib/sql/00_catalogs_and_tables.sql /opt/flink/usrlib/sql/${job} > ${run_file} && nohup /opt/flink/bin/sql-client.sh -f ${run_file} > ${log} 2>&1 &"
  echo "Submitted ${job}; container log: ${log}"
done

echo "Streaming jobs requested. Check http://127.0.0.1:8082"
