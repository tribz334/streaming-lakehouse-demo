#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

docker compose --profile core --profile olap --profile bi --profile ops \
  --profile governance --profile metastore --profile scheduler ps

check_url() {
  local name="$1"
  local url="$2"
  curl --fail --silent --show-error --max-time 10 "$url" >/dev/null
  echo "${name}: OK"
}

check_url "Flink" "http://127.0.0.1:8082/jobs"
check_url "Prometheus" "http://127.0.0.1:19090/-/ready"
check_url "Apicurio" "http://127.0.0.1:8081/apis/registry/v3/system/info"
check_url "Superset" "http://127.0.0.1:8088/health"
check_url "DolphinScheduler" "http://127.0.0.1:12345/dolphinscheduler/ui/"

echo "Stack verification finished."
