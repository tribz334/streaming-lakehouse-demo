# Current status

- Architecture: SDK JSON -> Fluss ODS, MySQL CDC -> Fluss ODS/DIM, Flink -> DWD/DWS/realtime ADS, Fluss Tiering -> Paimon, daily batch -> DM/offline ADS, StarRocks -> DBeaver/Superset.
- Classification: Unit owns `placement_type` (1=feed, 2=search, 3=splash, 4=rewarded, 5=banner, 6=other) and `ad_type` (1=short_video, 2=live, 3=image_text, 4=other). DWD enriches both fields through `creative_id`; SDK retains `slot_id` but does not report either classification.
- Cost: MySQL CDC writes `bill_info` directly to `dwd_ad_bill_di`; closed-loop Cost is derived before aggregation from `dim_unit.is_closed`.
- Realtime attribution: first paid orders use six-hour LastClick on `uid + product_id`; event, bill and signed order deltas enter the configurable 10-second event-time tumble `ads_realtime_metric_10s`.
- Offline attribution: `ads_order_attribution_di` uses `DIRECT/1D/7D/30D/ORGANIC`; there is no `LONG_TERM` bucket.
- Topics: DWS and DM each contain exactly advertiser / campaign / unit / creative tables. DM snapshots use `_1d`, `_7d`, `_30d`, and `_lifetime` metric suffixes.
- Persistence: realtime ADS tiers to Paimon with 5-second freshness; all other tier-enabled Fluss tables use 30 seconds.
- Money: raw Fluss/Paimon `BIGINT` values use one-thousandth of a fen (1 yuan = 100000). StarRocks business-serving views expose yuan.
- Dashboard: realtime reads only the latest 10-second window. Both dashboards expose Cost, closed-loop Cost, ad GMV, ROAS and non-`other` classification cards; offline additionally exposes CTR, CVR, Cost daily trend and GMV daily trend. Warehouse `other_*` fields remain available for reconciliation but are not mounted on either dashboard.
- Demo data: `event-generator` creates a deterministic, economically coherent 35-day history and then continues realtime traffic. `materialize-demo-history.ps1` publishes the requested offline date range.
- Verification: the supported local flow is `start-stack.ps1 -WithBi`, `materialize-demo-history.ps1`, then `verify-stack.ps1 -WithBi`.

For an existing demo volume whose runtime schema is incompatible, `start-stack.ps1 -WithBi -RebuildRuntimeSchema` stops streaming jobs and rebuilds the Fluss/Paimon runtime tables. Use it only after confirming that local demo data may be regenerated; it is not a production online-migration mechanism.

The checked-in Docker Compose topology is a single-node thesis demonstration: one Fluss Coordinator/Tablet, one Flink JobManager/TaskManager and one StarRocks FE/BE. The thesis production target requires replicated services, object storage, durable checkpoints/savepoints, managed secrets, orchestration, resource isolation, monitoring/alerting and backup/recovery. Demo credentials, generated history and destructive schema rebuilds are intentionally outside that production target.
