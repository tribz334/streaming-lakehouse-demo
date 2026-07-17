# Real Stack Status

Last verified: 2026-07-17 01:14 Asia/Shanghai.

## Verified Running

- Docker Desktop daemon is running.
- `core` profile is running:
  - MySQL 8.4 with binlog enabled
  - Apache Kafka 3.9.1
  - Flink 2.0.2 JobManager/TaskManager
  - Paimon connector in the Flink image
  - event generator writing Kafka `ods_log`
- Flink DDL created source tables and Paimon ODS/DIM/DWD/DWS/ADS tables.
- The operational chain includes persistent ODS, DWD, 10-second DWS, Kafka relay, and themed DWS jobs.
- DWS and ADS refresh jobs finish synchronously in batch mode:
  - DWS 10-second metric aggregation
  - advertiser retention
  - order-level LastClick attribution with mutually exclusive 30-minute, 1/3/7/30-day and organic buckets
  - demo-calibrated fraud signal detection
  - creative-grain offline BI serving dataset
- StarRocks FE/BE is running and queryable.
- `sync_dws_ad_metric_stream_10s` Routine Load is `RUNNING`; after restart with latest-only DWD scanning, a 25-second check advanced all three Kafka partitions and loaded 21,841 total records with zero rejected rows.
- Superset is running and includes the offline creative dataset:
  - `v_realtime_ad_metrics`
  - `v_advertiser_retention`
  - `v_attribution_summary`
  - `v_order_attribution_detail`
  - `v_creative_offline_metrics`
  - `v_fraud_signal_summary`
- Superset exposes attribution and anti-fraud as separate BI applications:
  - `广告归因分析大盘` (`/superset/dashboard/ad-attribution-analysis/`)
  - `广告反作弊监控大盘` (`/superset/dashboard/ad-fraud-monitoring/`)
- Prometheus is reachable at `http://127.0.0.1:19090/-/ready`.
- Apicurio Registry 3.2.5 is reachable at `http://127.0.0.1:8081/apis/registry/v3/system/info`.
- Apicurio has one registered JSON schema artifact: `ad-demo/ods_log-value`.
- Hive Metastore 4.0.1 is reachable on `127.0.0.1:19083`.
- DataHub-style offline governance metadata was exported to `datahub/metadata/lakehouse_metadata.json`.
- DataHub MCP-style JSONL was exported to `datahub/mcp/metadata_change_proposals.jsonl`.
- A local ops dashboard was generated at `ops-dashboard/index.html`.
- A local scheduler dashboard was generated at `dolphinscheduler/dashboard/index.html`.
- A DolphinScheduler workflow template is available at `dolphinscheduler/workflows/ad-lakehouse-demo.yaml`.

## Latest Paimon Counts

```text
ods_ad_events_di                179135
dwd_ad_events_di                 47552
dws_ad_metric_stream_10s         continuously updated by Flink
ads_advertiser_retention_di          1
ads_attribution_summary_di          14
ads_fraud_signal_di                 refreshed by batch workflow
dim_advertiser_df                   12
```

## Latest StarRocks View Counts

```text
v_realtime_ad_metrics       81232 at verification and continuously increasing
v_advertiser_retention          1
v_attribution_summary          14
v_creative_offline_metrics     refreshed by daily offline workflow
v_fraud_signal_summary         12
```

## Compatibility Notes

- The thesis text says Flink 2.0 + Paimon 1.0. Maven's Flink 2.0 bridge is available as `paimon-flink-2.0` from Paimon 1.1.x, so the runnable image uses `paimon-flink-2.0:1.1.1`.
- The initially tested `flink-sql-connector-mysql-cdc:3.5.0` failed on Flink 2.0.2 with `SourceFunction` compatibility. The runnable core path therefore uses Flink JDBC 4.0 for dimension bootstrap and keeps a CDC YAML sketch in `flink-cdc/mysql-to-paimon.yaml`.
- The real-time StarRocks path does not depend on Paimon External Catalog compatibility: Flink upsert-kafka and StarRocks Routine Load continuously serve `dws_ad_metric_stream_10s`. External Catalog remains available for compatibility experiments, and offline ADS still use snapshots.
- Fraud thresholds in `12_ads_fraud.sql` are calibrated for the local generator's injected fraud bursts. They are meant to demonstrate the rule pipeline, not to be production thresholds.
- Long-running Paimon stream readers can hit expired snapshots if an old job resumes from an expired checkpoint. The DWD job uses `scan.mode = latest` for the ODS source, and the current bad-state job was canceled/re-submitted.
- ADS tables are bounded batch outputs; `dws_ad_metric_stream_10s` and its StarRocks serving path are persistent streaming jobs.

## Remaining

- Grafana/Loki/Alloy runtime was validated on 2026-07-15. Alloy discovers the `ustc_lakehouse` Compose containers through the Docker API, Loki exposes the expected service/node/role labels, and Grafana reports the Loki data source as healthy.
- DolphinScheduler standalone runtime validation is still pending because the Docker image pull was interrupted by the same Docker Hub TLS timeout. Run-history JSON, scheduler dashboard, and the YAML workflow template are present as fallback artifacts.
- DataHub is not yet implemented as a running local profile; an offline DataHub-style metadata export and MCP-style JSONL export are present, and a real ingestion target can be added when a DataHub service is available.
- Strict MySQL CDC execution remains a compatibility item. The current demo keeps the CDC YAML sketch and uses JDBC bootstrap for a stable Flink 2.0.2 path.
- Direct StarRocks-to-Paimon querying should be retested with a newer StarRocks image when Docker Hub access is stable.
