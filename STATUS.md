# Real Stack Status

Last verified: 2026-07-19 10:36 Asia/Shanghai.

## Verified Running

- Docker Desktop daemon is running.
- `core` profile is running:
  - MySQL 8.4 with binlog enabled
  - Apache Kafka 3.9.1
  - Flink 2.2.0 JobManager/TaskManager
  - Paimon Flink 2.2 connector 1.4.2
  - Flink CDC 3.6.0-2.2 snapshot + binlog pipeline
  - event generator writing Kafka `ods_log`
- Flink DDL created source tables and Paimon ODS/DIM/DWD/DWS/ADS tables.
- The real-time hot path is one persistent Java Flink job: Kafka parsing, event-time windowing, metric aggregation, dimension enrichment, and one final StarRocks write.
- DWS and ADS refresh jobs finish synchronously in batch mode:
  - DWS 10-second metric aggregation
  - advertiser retention
  - order-level LastClick attribution with mutually exclusive 30-minute, 1/3/7/30-day and organic buckets
  - demo-calibrated fraud signal detection
  - creative-grain offline BI serving dataset
- StarRocks FE/BE is running and queryable.
- Java job `3ab08552c5725627876e79096411d6a5` was `RUNNING`; completed checkpoints advanced and the StarRocks table grew from 193,729 to 193,797 rows during verification.
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
dws_ad_metric_stream_10s         retained for offline compatibility; not used by the hot path
ads_advertiser_retention_di          1
ads_attribution_summary_di          14
ads_fraud_signal_di                 refreshed by batch workflow
dim_advertiser_df                   39
dim_campaign_df                     59
dim_unit_df                         76
dim_creative_df                     83
ods_ad_order                      1019
```

## Latest StarRocks View Counts

```text
v_realtime_ad_metrics      193797 at verification and continuously increasing
v_advertiser_retention          1
v_attribution_summary          14
v_creative_offline_metrics     refreshed by daily offline workflow
v_fraud_signal_summary         12
```

## Compatibility Notes

- The runnable version set is Flink 2.2.0, Flink CDC 3.6.0-2.2, Paimon Flink 2.2 bridge 1.4.2, and Kafka connector 5.0.0-2.2.
- `flink-cdc/mysql-to-paimon.yaml` is now a submitted persistent job. Full snapshot counts and insert/update/delete binlog propagation were verified on 2026-07-17.
- The real-time StarRocks path does not depend on Paimon External Catalog compatibility: one Java Flink job writes 10-second metrics directly through JDBC. External Catalog remains available for compatibility experiments, and offline ADS still use snapshots.
- Fraud thresholds in `12_ads_fraud.sql` are calibrated for the local generator's injected fraud bursts. They are meant to demonstrate the rule pipeline, not to be production thresholds.
- Long-running Paimon stream readers can hit expired snapshots if an old job resumes from an expired checkpoint. The DWD job uses `scan.mode = latest` for the ODS source, and the current bad-state job was canceled/re-submitted.
- ADS tables are bounded batch outputs; `RealtimeAdMetricJob` is the persistent streaming job that owns the StarRocks real-time metric table.

## Remaining

- Grafana/Loki/Alloy runtime was validated on 2026-07-15. Alloy discovers the `ustc_lakehouse` Compose containers through the Docker API, Loki exposes the expected service/node/role labels, and Grafana reports the Loki data source as healthy.
- DolphinScheduler standalone runtime validation is still pending because the Docker image pull was interrupted by the same Docker Hub TLS timeout. Run-history JSON, scheduler dashboard, and the YAML workflow template are present as fallback artifacts.
- DataHub is not yet implemented as a running local profile; an offline DataHub-style metadata export and MCP-style JSONL export are present, and a real ingestion target can be added when a DataHub service is available.
- Apicurio currently governs the Kafka `ods_log` JSON schema; it is not yet an inline serializer/validator for the direct MySQL-to-Paimon CDC path.
- Direct StarRocks-to-Paimon querying should be retested with a newer StarRocks image when Docker Hub access is stable.
