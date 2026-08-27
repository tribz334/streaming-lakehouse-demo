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
  "fluss-bill-ods-to-dwd:02b_ods_changelog_to_dwd.sql" \
  "fluss-order-ods-to-dwd-ad-order-di:02d_order_ods_to_dwd.sql" \
  "fluss-attributed-order-to-event-di:02e_attributed_order_to_event.sql" \
  "fluss-dwd-to-dws-topic-di:03_realtime_order_dws.sql"; do
  name="${spec%%:*}"; file="${spec#*:}"
  grep -q "$name" <<<"$running" || docker compose exec -d flink-jobmanager /bin/bash -lc \
    "nohup /opt/flink/bin/sql-client.sh -f /opt/flink/usrlib/sql/$file > /tmp/$name.log 2>&1 &"
done

jar="/opt/flink/usrlib/ad-realtime-datastream-jobs.jar"
for spec in \
  "fluss-ods-log-datastream-to-dwd:cn.edu.ustc.lakehouse.realtime.job.DwdLogDataStreamJob" \
  "fluss-last-click-datastream-6h:cn.edu.ustc.lakehouse.realtime.job.DwdOrderAttributionJob" \
  "fluss-realtime-metric-datastream-10s:cn.edu.ustc.lakehouse.realtime.job.RealtimeAdMetricJob"; do
  name="${spec%%:*}"; class="${spec#*:}"
  grep -q "$name" <<<"$running" || docker compose exec -T flink-jobmanager flink run -d \
    -c "$class" "$jar" --fluss-bootstrap fluss-coordinator:9123 --fluss-database ad_dw \
    --startup-mode earliest --out-of-orderness-seconds 10 \
    --attribution-allowed-lateness-seconds 10 --realtime-metric-window-seconds 10
done
