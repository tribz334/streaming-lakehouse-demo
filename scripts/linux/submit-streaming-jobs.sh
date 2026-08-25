#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
running="$(docker compose exec -T flink-jobmanager flink list -r 2>&1 || true)"
if ! grep -q "Fluss Lake Tiering Service" <<<"$running"; then
  docker compose exec -T flink-jobmanager flink run -d /opt/flink/opt/fluss-flink-tiering.jar \
    --fluss.bootstrap.servers fluss-coordinator:9123 --datalake.format paimon \
    --datalake.paimon.metastore filesystem --datalake.paimon.warehouse file:///warehouse/paimon
fi
for spec in "mysql-cdc-to-fluss-ods-and-dim:02_database_cdc_to_fluss.sql" \
  "fluss-ods-to-dwd-ad-event-and-bill-di:02b_ods_changelog_to_dwd.sql" \
  "fluss-order-direct-attribution-6h:02c_direct_order_attribution.sql" \
  "fluss-order-cdc-to-dwd-ad-order-di:02d_attributed_order_to_event.sql" \
  "fluss-dwd-to-three-dws-topic-di:03_realtime_order_dws.sql" \
  "fluss-dws-creative-di-to-ads-realtime-30s:04_realtime_ads.sql"; do
  name="${spec%%:*}"; file="${spec#*:}"
  grep -q "$name" <<<"$running" || docker compose exec -d flink-jobmanager /bin/bash -lc \
    "nohup /opt/flink/bin/sql-client.sh -f /opt/flink/usrlib/sql/$file > /tmp/$name.log 2>&1 &"
done
