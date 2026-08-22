#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

docker run --rm -v maven-cache:/root/.m2 -v "${ROOT}/flink-java:/workspace" \
  -w /workspace maven:3.9.11-eclipse-temurin-17 mvn -q -DskipTests package

if docker compose exec -T flink-jobmanager flink list -r 2>&1 | grep -q mysql-cdc-direct-to-dim; then
  echo "MySQL CDC direct-to-DIM job is already running."
else
  docker compose exec -d flink-jobmanager /bin/bash -lc \
    "nohup /opt/flink/bin/sql-client.sh -f /opt/flink/usrlib/sql/05_mysql_cdc_direct_to_dim.sql > /tmp/mysql-cdc-direct-to-dim.log 2>&1 &"
fi

if docker compose exec -T flink-jobmanager flink list -r 2>&1 | grep -q DwsAdMetric; then
  echo "Realtime Java attribution job is already running; no duplicate job was submitted."
else
  docker compose exec -T flink-jobmanager flink run -d \
    -c cn.edu.ustc.lakehouse.realtime.job.DwsAdMetric \
    /opt/flink/usrlib/java/ad-realtime-metric-job.jar \
    --startup-mode latest \
    --order-mysql-hostname mysql \
    --order-mysql-database ad_ods \
    --order-mysql-table order_detail \
    --order-mysql-server-id 5501-5508 \
    --order-mysql-startup-mode latest-offset \
    --parallelism 1
fi

if docker compose exec -T flink-jobmanager flink list -r 2>&1 | grep -q dwd-order-lifecycle-acc; then
  echo "Paimon accumulating order lifecycle job is already running."
else
  docker compose exec -d flink-jobmanager /bin/bash -lc \
    "nohup /opt/flink/bin/sql-client.sh -f /opt/flink/usrlib/sql/02_dwd_order_lifecycle.sql > /tmp/dwd-order-lifecycle-acc.log 2>&1 &"
fi

if docker compose exec -T flink-jobmanager flink list -r 2>&1 | grep -q ods-log-and-dwd-ad-facts; then
  echo "Paimon DWD advertising action/bill job is already running."
else
  docker compose exec -d flink-jobmanager /bin/bash -lc \
    "nohup /opt/flink/bin/sql-client.sh -f /opt/flink/usrlib/sql/04_dwd_ad_facts_to_paimon.sql > /tmp/dwd-ad-facts-to-paimon.log 2>&1 &"
fi

echo "Realtime attribution and all Paimon DWD materialization jobs submitted. Check http://127.0.0.1:18082"
