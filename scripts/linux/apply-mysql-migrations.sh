#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

for migration in \
  mysql/migrations/029_expand_order_lifecycle.sql \
  mysql/migrations/030_normalize_order_amount_fields.sql \
  mysql/migrations/031_add_order_shop_id.sql \
  mysql/migrations/032_enrich_creative_config.sql \
  mysql/migrations/034_repair_utf8_master_data.sql \
  mysql/migrations/035_remove_order_ad_source_fields.sql \
  mysql/migrations/036_simplify_order_lifecycle.sql \
  mysql/migrations/038_unit_classification.sql; do
  docker compose exec -T mysql mysql -uroot -proot --default-character-set=utf8mb4 < "$migration"
  echo "Applied $migration"
done
