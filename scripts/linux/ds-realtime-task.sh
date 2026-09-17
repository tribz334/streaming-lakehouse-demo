#!/usr/bin/env bash
set -euo pipefail

TASK="${1:?usage: ds-realtime-task.sh <task>}"
echo "task=$TASK"
# Serialize checks and detached submission across manual and scheduled runs.
exec 9>/tmp/ad-lakehouse-realtime.lock
flock -w 180 9
COMPOSE_PROJECT="${COMPOSE_PROJECT_NAME:-ustc_lakehouse}"

container_id() {
  docker ps -a \
    --filter "label=com.docker.compose.project=${COMPOSE_PROJECT}" \
    --filter "label=com.docker.compose.service=$1" \
    --format '{{.ID}}' | head -n1
}

require_running() {
  local id
  id="$(container_id "$1")"
  test -n "$id" || { echo "missing Compose service container: $1" >&2; exit 1; }
  test "$(docker inspect -f '{{.State.Running}}' "$id")" = "true" || {
    echo "Compose service is not running: $1" >&2
    exit 1
  }
  printf '%s' "$id"
}

flink_jobs() {
  docker exec "$(require_running flink-jobmanager)" curl -fsS http://localhost:8081/jobs/overview |
    jq -er '[.jobs[] | select(.state == "RUNNING") | .name] | join("\n")' 
}

wait_for_job() {
  local name="$1"
  for _ in $(seq 1 30); do
    if flink_jobs | grep -Fxq "$name"; then
      echo "Flink job is running: $name"
      return 0
    fi
    sleep 2
  done
  echo "Flink job did not reach RUNNING state: $name" >&2
  return 1
}

submit_java_job() {
  local name="$1" class="$2" jm jobs
  jm="$(require_running flink-jobmanager)"
  jobs="$(flink_jobs)"
  if grep -Fxq "$name" <<<"$jobs"; then
    echo "Flink job already running; submission skipped: $name"
    return 0
  fi
  docker exec "$jm" flink run -d -m flink-jobmanager:8081 -Dexecution.runtime-mode=STREAMING -c "$class" \
    /opt/flink/usrlib/ad-realtime-datastream-jobs.jar \
    --fluss-bootstrap fluss-coordinator:9123 --fluss-database ad_dw \
    --startup-mode earliest --out-of-orderness-seconds 10 \
    --attribution-allowed-lateness-seconds 10 --realtime-metric-window-seconds 10
  wait_for_job "$name"
}

case "$TASK" in
  check_core)
    for service in mysql fluss-coordinator fluss-tablet flink-jobmanager flink-taskmanager starrocks starrocks-be; do
      require_running "$service" >/dev/null
      echo "service ready: $service"
    done
    ;;
  prepare_tables)
    jm="$(require_running flink-jobmanager)"
    docker exec "$jm" /opt/flink/bin/sql-client.sh -f /opt/flink/usrlib/sql/00_bootstrap.sql
    ;;
  start_tiering)
    jm="$(require_running flink-jobmanager)"
    if flink_jobs | grep -Fxq 'Fluss Lake Tiering Service'; then
      echo 'Fluss Lake Tiering Service already running; submission skipped.'
    else
      docker exec "$jm" flink run -d /opt/flink/opt/fluss-flink-tiering.jar \
        --fluss.bootstrap.servers fluss-coordinator:9123 \
        --datalake.format paimon --datalake.paimon.metastore filesystem \
        --datalake.paimon.warehouse file:///warehouse/paimon
      wait_for_job 'Fluss Lake Tiering Service'
    fi
    ;;
  start_mysql_cdc)
    name='mysql-cdc-to-fluss-ods-and-dim'
    jm="$(require_running flink-jobmanager)"
    if flink_jobs | grep -Fxq "$name"; then
      echo "Flink job already running; submission skipped: $name"
    else
      docker exec -d "$jm" /bin/bash -lc \
        "nohup /opt/flink/bin/sql-client.sh -f /opt/flink/usrlib/sql/02_database_cdc_to_fluss.sql > /tmp/${name}.log 2>&1 &"
      wait_for_job "$name"
    fi
    ;;
  start_sdk_collection)
    generator="$(container_id event-generator)"
    test -n "$generator" || { echo 'event-generator container has not been created; run start-stack first.' >&2; exit 1; }
    if test "$(docker inspect -f '{{.State.Running}}' "$generator")" != "true"; then
      docker start "$generator" >/dev/null
    fi
    echo "SDK event generator running: $generator"
    ;;
  start_dwd_event)
    submit_java_job 'fluss-ods-log-datastream-to-dwd' \
      'cn.edu.ustc.lakehouse.realtime.job.DwdLogDataStreamJob'
    ;;
  start_dwd_bill)
    submit_java_job 'fluss-dwd-ad-bill-enrichment' \
      'cn.edu.ustc.lakehouse.realtime.job.DwdAdBillJob'
    ;;
  start_attribution)
    submit_java_job 'fluss-order-fact-datastream' \
      'cn.edu.ustc.lakehouse.realtime.job.DwdOrderAttributionJob'
    ;;
  start_realtime_dws)
    submit_java_job 'fluss-realtime-metric-datastream-10s' \
      'cn.edu.ustc.lakehouse.realtime.job.DwsAdCreativeJob'
    ;;
  verify_outputs)
    jobs="$(flink_jobs)"
    for name in \
      'Fluss Lake Tiering Service' \
      'mysql-cdc-to-fluss-ods-and-dim' \
      'fluss-ods-log-datastream-to-dwd' \
      'fluss-dwd-ad-bill-enrichment' \
      'fluss-order-fact-datastream' \
      'fluss-realtime-metric-datastream-10s'; do
      grep -Fxq "$name" <<<"$jobs" || { echo "missing RUNNING Flink job: $name" >&2; exit 1; }
    done
    for table in ods_log ods_mysql_bill_info dim_creative_df dwd_ad_event_di dwd_ad_bill_di dwd_order_acc; do
      path="/workspace/project/runtime-data/warehouse/paimon/ad_dw.db/$table"
      test -d "$path" || { echo "Paimon tiering target not found: $path" >&2; exit 1; }
      echo "Paimon table present: $table"
    done
    sr="$(require_running starrocks)"
    count="$(docker exec "$sr" mysql -N -h127.0.0.1 -P9030 -uroot -e \
      'SELECT COUNT(*) FROM ad_ads.v_realtime_metric;' 2>/dev/null | tail -n1)"
    test "${count:-0}" -gt 0 || { echo 'StarRocks realtime view has no rows.' >&2; exit 1; }
    mkdir -p /workspace/dolphinscheduler/runs
    printf 'realtime workflow verified at %s; starrocks_rows=%s\n' \
      "$(date -Iseconds)" "$count" | tee /workspace/dolphinscheduler/runs/realtime-workflow-execution.txt
    ;;
  *)
    echo "unknown realtime task: $TASK" >&2
    exit 2
    ;;
esac
