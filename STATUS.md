# Real Stack Status

Last verified: 2026-06-26 23:18 Asia/Shanghai.

## Verified Running

- Docker Desktop daemon is running.
- `core` profile is running:
  - MySQL 8.4 with binlog enabled
  - Apache Kafka 3.9.1
  - Flink 2.0.2 JobManager/TaskManager
  - Paimon connector in the Flink image
  - event generator writing Kafka `ods_log`
- Flink DDL created source tables and Paimon ODS/DIM/DWD/DWS/ADS tables.
- Two long-running Flink jobs are running:
  - Kafka/JDBC ingestion to Paimon ODS/DIM/order lifecycle
  - DWD enrichment
- DWS and ADS refresh jobs finish synchronously in batch mode:
  - DWS 10-second metric aggregation
  - advertiser retention
  - last-click attribution in a 7-day window
  - demo-calibrated fraud signal detection
- StarRocks FE/BE is running and queryable.
- Superset is running and has four registered StarRocks datasets:
  - `v_realtime_ad_metrics`
  - `v_advertiser_retention`
  - `v_attribution_summary`
  - `v_fraud_signal_summary`
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
dws_ad_metric_10s                refreshed by batch workflow
ads_advertiser_retention_di          1
ads_attribution_summary_di          14
ads_fraud_signal_di                 refreshed by batch workflow
dim_advertiser_df                   12
```

## Latest StarRocks View Counts

```text
v_realtime_ad_metrics          12
v_advertiser_retention          1
v_attribution_summary          14
v_fraud_signal_summary         12
```

## Compatibility Notes

- The thesis text says Flink 2.0 + Paimon 1.0. Maven's Flink 2.0 bridge is available as `paimon-flink-2.0` from Paimon 1.1.x, so the runnable image uses `paimon-flink-2.0:1.1.1`.
- The initially tested `flink-sql-connector-mysql-cdc:3.5.0` failed on Flink 2.0.2 with `SourceFunction` compatibility. The runnable core path therefore uses Flink JDBC 4.0 for dimension bootstrap and keeps a CDC YAML sketch in `flink-cdc/mysql-to-paimon.yaml`.
- StarRocks 3.1 can create a Paimon external catalog but cannot directly read the local Paimon 1.1.1 snapshot metadata field `baseManifestListSize`. The demo therefore syncs Paimon DWS/ADS outputs into StarRocks internal snapshot tables.
- Fraud thresholds in `06_ads_fraud_batch.sql` are calibrated for the local generator's injected fraud bursts. They are meant to demonstrate the rule pipeline, not to be production thresholds.
- Long-running Paimon stream readers can hit expired snapshots if an old job resumes from an expired checkpoint. The DWD job uses `scan.mode = latest` for the ODS source, and the current bad-state job was canceled/re-submitted.
- DWS and ADS are intentionally refreshed as bounded batch jobs in the local workflow. This keeps the single TaskManager stable while preserving the thesis pipeline layers.

## Remaining

- Grafana/Loki runtime validation is still pending because the image pull failed again on Docker Hub TLS handshake timeout at 2026-06-26 23:03. Config and dashboard skeletons are present, and `ops-dashboard/index.html` is the verified local fallback dashboard.
- DolphinScheduler standalone runtime validation is still pending because the Docker image pull was interrupted by the same Docker Hub TLS timeout. Run-history JSON, scheduler dashboard, and the YAML workflow template are present as fallback artifacts.
- DataHub is not yet implemented as a running local profile; an offline DataHub-style metadata export and MCP-style JSONL export are present, and a real ingestion target can be added when a DataHub service is available.
- Strict MySQL CDC execution remains a compatibility item. The current demo keeps the CDC YAML sketch and uses JDBC bootstrap for a stable Flink 2.0.2 path.
- Direct StarRocks-to-Paimon querying should be retested with a newer StarRocks image when Docker Hub access is stable.
