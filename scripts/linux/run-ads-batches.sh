#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

for job in \
  08_offline_dws.sql \
  09_offline_dm.sql \
  10_ads_retention.sql \
  11_ads_attribution.sql \
  12_ads_fraud.sql \
  13_ads_creative_offline.sql; do
  echo "Running ${job}..."
  run_file="/tmp/run_${job}"
  docker compose exec -T flink-jobmanager /bin/bash -lc \
    "cat /opt/flink/usrlib/sql/00_catalogs_and_tables.sql /opt/flink/usrlib/sql/01_model_tables.sql /opt/flink/usrlib/sql/${job} > ${run_file} && /opt/flink/bin/sql-client.sh -f ${run_file}"
done

echo "Streamlined DWS/DM/ADS batches finished."
