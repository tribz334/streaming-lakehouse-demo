# Current status

- Architecture: Flink + Fluss + Paimon + StarRocks.
- Ingestion: SDK JSON -> Fluss `ods_log_di`; MySQL CDC -> Fluss `ods_mysql_bill_di`, `ods_mysql_order_acc` and seven `dim_*_df` tables.
- Model: three core ODS tables + 7 DIM + order/bill/action DWD + attributed PAY/REFUND event + order GMV DWS/DM/ADS.
- Attribution: six-hour Last Click on `uid + product_id`; source orders contain no `creative_id` or `slot_id`.
- Realtime: DWD facts -> `dws_advertiser_di` / `dws_unit_di` / `dws_creative_di` -> `ads_realtime_metric_30s`.
- DM: `dm_advertiser_df` / `dm_unit_df` / `dm_creative_df`, with `_1d`, `_7d`, `_30d`, and `_acc` metric columns.
- Offline ADS: `ads_offline_metric_di`, advertiser retention `ads_advertiser_retention_di`, and per-order all-history Last Click `ads_order_attribution_di` with a `LONG_TERM` bucket beyond 30 days.
- Persistence: Fluss Tiering Service -> Paimon, target freshness 30 seconds.
- Offline: one business date per invocation, idempotently published to Paimon and StarRocks.
- DBeaver: connect to StarRocks `127.0.0.1:19030` and query the stable `ad_ads` service tables.
- DBeaver: `paimon_catalog` is permanent and can be refreshed to inspect tiered tables.
- Legacy simplified schemas and redundant Kafka/Java/HMS/manual-sync paths removed.

Runtime verification completed locally on 2026-08-24: Tiering and all six streaming jobs are RUNNING; all seven `dim_*_df` tables and the three DM snapshots are non-empty and queryable through StarRocks.
