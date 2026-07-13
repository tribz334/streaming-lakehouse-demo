#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

for job in \
  03_dws_metrics.sql \
  07_thesis_offline_layers.sql \
  04_ads_retention_batch.sql \
  05_ads_attribution_batch.sql \
  06_ads_fraud_batch.sql \
  08_data_quality_batch.sql; do
  echo "Running ${job}..."
  run_file="/tmp/run_${job}"
  docker compose --profile core exec -T flink-jobmanager /bin/bash -lc \
    "cat /opt/flink/usrlib/sql/00_catalogs_and_tables.sql /opt/flink/usrlib/sql/${job} > ${run_file} && /opt/flink/bin/sql-client.sh -f ${run_file}"
done

echo "Thesis DWD/DWM/DWS/DM layers and ADS batches finished."
