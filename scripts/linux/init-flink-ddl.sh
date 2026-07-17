#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

docker compose exec -T flink-jobmanager /bin/bash -lc \
  "/opt/flink/bin/sql-client.sh -f /opt/flink/usrlib/sql/00_catalogs_and_tables.sql"

docker compose exec -T flink-jobmanager /bin/bash -lc \
  "/opt/flink/bin/sql-client.sh -f /opt/flink/usrlib/sql/01_model_tables.sql"

echo "Flink catalogs and thesis Appendix A tables initialized."
