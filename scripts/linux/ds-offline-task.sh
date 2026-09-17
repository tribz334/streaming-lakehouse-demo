#!/usr/bin/env bash
set -euo pipefail

TASK="${1:?usage: ds-offline-task.sh <task> <yyyy-MM-dd>}"
BIZ_DATE="${2:?missing biz_date}"
[[ "$BIZ_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || { echo "invalid biz_date: $BIZ_DATE" >&2; exit 2; }
date -d "$BIZ_DATE" +%F >/dev/null

echo "task=$TASK biz_date=$BIZ_DATE"

COMPOSE_PROJECT="${COMPOSE_PROJECT_NAME:-ustc_lakehouse}"
PREV_DATE="$(date -d "$BIZ_DATE -1 day" +%F)"
DATE_MINUS_7="$(date -d "$BIZ_DATE -7 day" +%F)"
DATE_MINUS_30="$(date -d "$BIZ_DATE -30 day" +%F)"
BIZ_DATE_COMPACT="$(date -d "$BIZ_DATE" +%Y%m%d)"

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

render_sql() {
  local source="$1" target="$2"
  sed -e "s/__BIZ_DATE_COMPACT__/$BIZ_DATE_COMPACT/g" \
      -e "s/__BIZ_DATE__/$BIZ_DATE/g" \
      -e "s/__PREV_DATE__/$PREV_DATE/g" \
      -e "s/__DATE_MINUS_7__/$DATE_MINUS_7/g" \
      -e "s/__DATE_MINUS_30__/$DATE_MINUS_30/g" \
      "$source" | awk -v stage="${SQL_STAGE:-all}" '
        /^INSERT OVERWRITE paimon[.]ad_dw[.]/ {
          if ($0 ~ /[.]dm_/) section="dm";
          else if ($0 ~ /[.]ads_/) section="ads";
          else section="other";
        }
        stage == "all" || section == "" || section == stage { print }
      ' > "$target"
}

run_sql() {
  local source="$1" remote="$2" jm tmp output
  jm="$(require_running flink-jobmanager)"
  tmp="$(mktemp)"
  trap 'rm -f -- "${tmp:-}"' RETURN
  render_sql "$source" "$tmp"
  docker cp "$tmp" "$jm:$remote"
  set +e
  output="$(docker exec "$jm" /opt/flink/bin/sql-client.sh -f "$remote" 2>&1)"
  status=$?
  set -e
  printf '%s\n' "$output"
  test "$status" -eq 0
  ! grep -Fq '[ERROR]' <<<"$output"
}

case "$TASK" in
  check_inputs)
    require_running flink-jobmanager >/dev/null
    require_running starrocks >/dev/null
    base='/workspace/project/runtime-data/warehouse/paimon/ad_dw.db'
    for table in dwd_ad_event_di dwd_ad_bill_di dwd_order_acc; do
      test -d "$base/$table" || { echo "missing Paimon DWD table: $table" >&2; exit 1; }
      echo "Paimon input present: $table"
    done
    echo "offline business date: $BIZ_DATE"
    ;;
  run_dws)
    run_sql /workspace/project/flink/sql/04_daily_dws.sql "/tmp/ds-dws-${BIZ_DATE}.sql"
    ;;
  run_dm|run_ads)
    SQL_STAGE="${TASK#run_}"
    run_sql /workspace/project/flink/sql/10_daily_offline.sql "/tmp/ds-${SQL_STAGE}-${BIZ_DATE}.sql"
    ;;
  run_dm_ads)
    # The current project intentionally keeps DM and ADS in one synchronized
    # statement file; this node does not pretend that independent jobs exist.
    run_sql /workspace/project/flink/sql/10_daily_offline.sql "/tmp/ds-dm-ads-${BIZ_DATE}.sql"
    ;;
  verify_outputs)
    sr="$(require_running starrocks)"
    queries=(
      "SELECT COUNT(*) FROM ad_ads.v_dws_ad_creative_di WHERE dt='$BIZ_DATE'"
      "SELECT COUNT(*) FROM ad_ads.v_dm_ad_creative_df WHERE dt='$BIZ_DATE'"
      "SELECT COUNT(*) FROM ad_ads.v_ads_offline_metric_di WHERE dt='$BIZ_DATE'"
      "SELECT COUNT(*) FROM ad_ads.v_ads_order_attribution_di WHERE dt='$BIZ_DATE'"
      "SELECT COUNT(*) FROM ad_ads.v_ads_advertiser_retention_di WHERE dt='$BIZ_DATE'"
    )
    total=0
    for sql in "${queries[@]}"; do
      value="$(docker exec "$sr" mysql -N -h127.0.0.1 -P9030 -uroot -e "$sql" 2>/dev/null | tail -n1)"
      test "${value:-0}" -gt 0 || { echo "offline verification returned no rows: $sql" >&2; exit 1; }
      echo "$sql => $value"
      total=$((total + value))
    done
    mkdir -p /workspace/dolphinscheduler/runs
    printf 'offline workflow verified at %s; biz_date=%s; checked_rows=%s\n' \
      "$(date -Iseconds)" "$BIZ_DATE" "$total" | tee /workspace/dolphinscheduler/runs/offline-workflow-execution.txt
    ;;
  *)
    echo "unknown offline task: $TASK" >&2
    exit 2
    ;;
esac
