SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

TRUNCATE TABLE paimon.ad_dw.ads_data_quality_result_di;
TRUNCATE TABLE paimon.ad_dw.ads_data_quality_summary_di;

CREATE TEMPORARY VIEW quality_rules AS
WITH stats AS (
  SELECT
    (SELECT COUNT(*) FROM paimon.ad_dw.ods_ad_events_di) AS ods_count,
    (SELECT COUNT(*) FROM paimon.ad_dw.dwd_ad_events_di) AS dwd_count,
    (SELECT COUNT(*) FROM paimon.ad_dw.dwd_ad_events_di WHERE advertiser_id IS NULL OR campaign_id IS NULL OR unit_id IS NULL OR creative_id IS NULL) AS null_dimension_count,
    (SELECT COUNT(*) FROM paimon.ad_dw.dwd_ad_events_di WHERE spend < 0 OR gmv < 0) AS negative_amount_count,
    (SELECT COUNT(*) FROM paimon.ad_dw.dwm_ad_event_wide) AS dwm_count,
    (SELECT COUNT(*) FROM paimon.ad_dw.dws_advertiser_df) AS thesis_dws_count,
    (SELECT COUNT(*) FROM (
      SELECT conversion_id FROM paimon.ad_dw.dm_attribution_touchpoint_df
      WHERE is_last_click GROUP BY conversion_id HAVING COUNT(*) > 1
    )) AS duplicate_attribution_count,
    (SELECT COUNT(*) FROM paimon.ad_dw.dws_ad_metric_10s WHERE ctr < 0 OR ctr > 1) AS invalid_ctr_count,
    (SELECT COUNT(*) FROM paimon.ad_dw.dws_ad_metric_10s WHERE cvr < 0 OR cvr > 1) AS invalid_cvr_count,
    (SELECT COUNT(*) FROM paimon.ad_dw.dws_ad_metric_10s WHERE roi < 0) AS invalid_roi_count
)
SELECT * FROM (
  SELECT 'DQ001' AS rule_code, 'ODS non-empty' AS rule_name, 'ODS' AS data_layer, 'ods_ad_events_di' AS target_table,
         CAST(ods_count AS DECIMAL(24,6)) AS actual_value, '> 0' AS expected_value,
         CASE WHEN ods_count > 0 THEN 'PASS' ELSE 'FAIL' END AS check_status, 'CRITICAL' AS severity,
         'Raw event table must contain data.' AS details FROM stats
  UNION ALL
  SELECT 'DQ002', 'DWD non-empty', 'DWD', 'dwd_ad_events_di', CAST(dwd_count AS DECIMAL(24,6)), '> 0',
         CASE WHEN dwd_count > 0 THEN 'PASS' ELSE 'FAIL' END, 'CRITICAL', 'Enriched detail table must contain data.' FROM stats
  UNION ALL
  SELECT 'DQ003', 'ODS-DWD row count variance', 'DWD', 'dwd_ad_events_di',
         CAST(ABS(ods_count - dwd_count) AS DECIMAL(24,6)), '<= 5% of ODS rows',
         CASE WHEN ods_count > 0 AND ABS(ods_count - dwd_count) * 1.0 / ods_count <= 0.05 THEN 'PASS' ELSE 'FAIL' END,
         'CRITICAL', 'Detects event loss or duplicated enrichment.' FROM stats
  UNION ALL
  SELECT 'DQ004', 'Required dimensions complete', 'DWD', 'dwd_ad_events_di', CAST(null_dimension_count AS DECIMAL(24,6)), '= 0',
         CASE WHEN null_dimension_count = 0 THEN 'PASS' ELSE 'FAIL' END, 'HIGH', 'Advertiser, campaign, unit and creative IDs are required.' FROM stats
  UNION ALL
  SELECT 'DQ005', 'Amounts non-negative', 'DWD', 'dwd_ad_events_di', CAST(negative_amount_count AS DECIMAL(24,6)), '= 0',
         CASE WHEN negative_amount_count = 0 THEN 'PASS' ELSE 'FAIL' END, 'HIGH', 'Spend and GMV cannot be negative.' FROM stats
  UNION ALL
  SELECT 'DQ006', 'CTR range valid', 'DWS', 'dws_ad_metric_10s', CAST(invalid_ctr_count AS DECIMAL(24,6)), '= 0',
         CASE WHEN invalid_ctr_count = 0 THEN 'PASS' ELSE 'FAIL' END, 'MEDIUM', 'CTR must be between zero and one.' FROM stats
  UNION ALL
  SELECT 'DQ007', 'CVR range valid', 'DWS', 'dws_ad_metric_10s', CAST(invalid_cvr_count AS DECIMAL(24,6)), '= 0',
         CASE WHEN invalid_cvr_count = 0 THEN 'PASS' ELSE 'FAIL' END, 'MEDIUM', 'CVR must be between zero and one.' FROM stats
  UNION ALL
  SELECT 'DQ008', 'ROI non-negative', 'DWS', 'dws_ad_metric_10s', CAST(invalid_roi_count AS DECIMAL(24,6)), '= 0',
         CASE WHEN invalid_roi_count = 0 THEN 'PASS' ELSE 'FAIL' END, 'MEDIUM', 'ROI cannot be negative.' FROM stats
  UNION ALL
  SELECT 'DQ009', 'DWM shared wide table non-empty', 'DWM', 'dwm_ad_event_wide', CAST(dwm_count AS DECIMAL(24,6)), '> 0',
         CASE WHEN dwm_count > 0 THEN 'PASS' ELSE 'FAIL' END, 'HIGH', 'Offline shared detail processing must be materialized.' FROM stats
  UNION ALL
  SELECT 'DQ010', 'Thesis DWS advertiser subject non-empty', 'DWS', 'dws_advertiser_df', CAST(thesis_dws_count AS DECIMAL(24,6)), '> 0',
         CASE WHEN thesis_dws_count > 0 THEN 'PASS' ELSE 'FAIL' END, 'HIGH', 'The canonical advertiser subject table must contain data.' FROM stats
  UNION ALL
  SELECT 'DQ011', 'Last-click attribution uniqueness', 'DM', 'dm_attribution_touchpoint_df', CAST(duplicate_attribution_count AS DECIMAL(24,6)), '= 0',
         CASE WHEN duplicate_attribution_count = 0 THEN 'PASS' ELSE 'FAIL' END, 'CRITICAL', 'Each outcome may have at most one last-click touchpoint.' FROM stats
) rules;

INSERT INTO paimon.ad_dw.ads_data_quality_result_di
SELECT
  CAST(CURRENT_DATE AS STRING) AS check_date,
  rule_code,
  rule_name,
  data_layer,
  target_table,
  actual_value,
  expected_value,
  check_status,
  severity,
  details,
  CURRENT_TIMESTAMP AS checked_at
FROM quality_rules;

INSERT INTO paimon.ad_dw.ads_data_quality_summary_di
SELECT
  CAST(CURRENT_DATE AS STRING) AS check_date,
  COUNT(*) AS total_rules,
  SUM(CASE WHEN check_status = 'PASS' THEN 1 ELSE 0 END) AS passed_rules,
  SUM(CASE WHEN check_status = 'FAIL' THEN 1 ELSE 0 END) AS failed_rules,
  CAST(SUM(CASE WHEN check_status = 'PASS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(18,2)) AS quality_score,
  CASE WHEN SUM(CASE WHEN check_status = 'FAIL' THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END AS overall_status,
  CURRENT_TIMESTAMP AS checked_at
FROM quality_rules;
