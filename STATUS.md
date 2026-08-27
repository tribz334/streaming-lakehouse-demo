# Current status

- Architecture: Flink + Fluss + Paimon + StarRocks.
- Ingestion: SDK JSON -> Fluss `ods_log_di`; MySQL CDC -> Fluss `ods_mysql_bill_di`, `ods_mysql_order_acc` and seven `dim_*_df` tables.
- Model: Unit-owned `placement_type` (1..6) and `ad_type` (1..4); SDK keeps `slot_id` but does not report classification. DWD enriches from `dim_unit_df` and persists the result.
- Attribution: six-hour LastClick on `uid + product_id`, aligned with the Fluss DWD hot-log retention; source orders contain no `creative_id` or `slot_id`.
- Realtime: Fluss DWD facts -> DataStream signed deltas -> configurable 10-second event-time tumble -> `ads_realtime_metric_10s`.
- DM: advertiser / campaign / unit / creative snapshots use only `_1d`, `_7d`, `_30d`, and `_lifetime` metric suffixes.
- Offline ADS: `ads_offline_metric_di`, advertiser retention `ads_advertiser_retention_di`, and per-order all-history Last Click `ads_order_attribution_di` with a `LONG_TERM` bucket beyond 30 days.
- Persistence: Fluss Tiering Service -> Paimon, target freshness 30 seconds.
- Offline: one business date per invocation, idempotently published to Paimon and StarRocks.
- DBeaver: connect to StarRocks `127.0.0.1:19030` and query the stable `ad_ads` service tables.
- DBeaver: `paimon_catalog` is permanent and can be refreshed to inspect tiered tables.
- Legacy Kafka, redundant SQL realtime jobs, HMS, and manual-sync paths removed; Java DataStream reads and writes Fluss through Table API.

Runtime metadata has been migrated to the row-oriented DWS/DM classification contract. StarRocks exposes all six DWS views and six DM views through `ad_ads`.
