#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

if curl -fsS http://127.0.0.1:8082/jobs/overview | grep -q '"name":"mysql-cdc-to-paimon".*"state":"RUNNING"'; then
  echo "Flink CDC pipeline is already running."
  exit 0
fi

docker compose exec -d flink-jobmanager /bin/bash -lc \
  "nohup /opt/flink-cdc/bin/flink-cdc.sh /opt/flink-cdc/pipelines/mysql-to-paimon.yaml --flink-home /opt/flink -t remote -Drest.address=flink-jobmanager -Drest.port=8081 >/tmp/mysql-cdc-to-paimon.log 2>&1 &"

for _ in $(seq 1 40); do
  if curl -fsS http://127.0.0.1:8082/jobs/overview | grep -q '"name":"mysql-cdc-to-paimon".*"state":"RUNNING"'; then
    echo "Flink CDC pipeline is running."
    exit 0
  fi
  sleep 3
done

docker compose exec -T flink-jobmanager /bin/bash -lc "tail -n 120 /tmp/mysql-cdc-to-paimon.log"
echo "Flink CDC pipeline did not reach RUNNING within two minutes." >&2
exit 1
