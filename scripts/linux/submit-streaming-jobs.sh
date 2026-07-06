#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

for job in 01_ingest_to_paimon.sql 02_dwd_enrich.sql 03a_dws_metrics_streaming.sql; do
  log="/tmp/${job}.log"
  run_file="/tmp/run_${job}"
  docker compose --profile core exec -d flink-jobmanager /bin/bash -lc \
    "cat /opt/flink/usrlib/sql/00_catalogs_and_tables.sql /opt/flink/usrlib/sql/${job} > ${run_file} && nohup /opt/flink/bin/sql-client.sh -f ${run_file} > ${log} 2>&1 &"
  echo "Submitted ${job}; container log: ${log}"
done

echo "Streaming jobs requested. Check http://127.0.0.1:8082"
