#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

docker run --rm -v maven-cache:/root/.m2 -v "${ROOT}/flink-java:/workspace" \
  -w /workspace maven:3.9.11-eclipse-temurin-17 mvn -q -DskipTests package

if docker compose exec -T flink-jobmanager flink list -r 2>&1 | grep -q starrocks_realtime_attribution_metric_sink; then
  echo "Realtime Java attribution job is already running; no duplicate job was submitted."
  exit 0
fi

docker compose exec -T flink-jobmanager flink run -d \
  -c cn.edu.ustc.lakehouse.realtime.DwsAdMetric \
  /opt/flink/usrlib/java/ad-realtime-metric-job.jar \
  --startup-mode latest \
  --order-mysql-hostname mysql \
  --order-mysql-database ad_ods \
  --order-mysql-table order \
  --order-mysql-server-id 5501-5508 \
  --order-mysql-startup-mode latest-offset \
  --parallelism 1

echo "Kafka ad_log + MySQL CDC order Last Click job submitted. Check http://127.0.0.1:8082"
