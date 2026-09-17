import json
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP = (ROOT / "flink/sql/00_bootstrap.sql").read_text(encoding="utf-8")
STARROCKS = (ROOT / "starrocks/init_starrocks.sql").read_text(encoding="utf-8")
SUBMIT_PS = (ROOT / "scripts/windows/submit-streaming-jobs.ps1").read_text(encoding="utf-8")
POM = (ROOT / "flink-java/pom.xml").read_text(encoding="utf-8")
CONFIG = (ROOT / "flink-java/src/main/java/cn/edu/ustc/lakehouse/realtime/config/RealtimeJobConfig.java").read_text(encoding="utf-8")
REALTIME_JOB = (ROOT / "flink-java/src/main/java/cn/edu/ustc/lakehouse/realtime/job/DwsAdCreativeJob.java").read_text(encoding="utf-8")
BILL_MAPPER = (ROOT / "flink-java/src/main/java/cn/edu/ustc/lakehouse/realtime/dws/BillFieldMapper.java").read_text(encoding="utf-8")
DWS_SQL = (ROOT / "flink/sql/04_daily_dws.sql").read_text(encoding="utf-8")
CDC_SQL = (ROOT / "flink/sql/02_database_cdc_to_fluss.sql").read_text(encoding="utf-8")
ATTRIBUTION = (ROOT / "flink-java/src/main/java/cn/edu/ustc/lakehouse/realtime/dwd/LastClickAttributionFunction.java").read_text(encoding="utf-8")
ORDER_JOB = (ROOT / "flink-java/src/main/java/cn/edu/ustc/lakehouse/realtime/job/DwdOrderAttributionJob.java").read_text(encoding="utf-8")
GENERATOR = (ROOT / "generator/produce_events.py").read_text(encoding="utf-8")
SCHEMA = json.loads((ROOT / "schemas/ods_log.schema.json").read_text(encoding="utf-8"))


class RuntimeContractTest(unittest.TestCase):
    def test_realtime_table_is_10_seconds_end_to_end(self):
        self.assertIn("CREATE TABLE IF NOT EXISTS dws_ad_creative_10s", BOOTSTRAP)
        self.assertNotIn("CREATE TABLE IF NOT EXISTS ads_realtime_metric_30s", BOOTSTRAP)
        self.assertIn("dws_ad_creative_10s", STARROCKS)
        self.assertIn("dws_ad_creative_10s", STARROCKS)
        self.assertIn("--realtime-metric-window-seconds", SUBMIT_PS)
        self.assertRegex(SUBMIT_PS, r"realtime-metric-window-seconds\s+10")

    def test_window_is_configurable_and_defaults_to_10(self):
        self.assertIn('getOrDefault("realtime-metric-window-seconds", "10")', CONFIG)
        self.assertIn("Time.seconds(config.realtimeMetricWindowSeconds())", REALTIME_JOB)
        self.assertNotRegex(REALTIME_JOB, r"Time\.seconds\(30\)|Duration\.ofSeconds\(30\)")

    def test_last_click_remains_six_hours(self):
        self.assertIn("Duration.ofHours(6)", ATTRIBUTION)
        self.assertNotIn("Duration.ofMinutes(10)", ATTRIBUTION)
        self.assertIn("ListState<AdClickEvent> clickHistoryState", ATTRIBUTION)
        self.assertIn("ListState<OrderInfo> pendingOrderState", ATTRIBUTION)
        self.assertIn("latestEligibleClick(clickHistoryState.get(), order.payTimeMillis)", ATTRIBUTION)
        self.assertNotIn(".dwd_ad_order_di", ORDER_JOB)
        self.assertIn("OrderProcessFunction.paidOrders()", ORDER_JOB)
        self.assertIn("LastClickAttributionFunction", ORDER_JOB)
        self.assertIn(".dwd_ad_order_acc", ORDER_JOB)
        self.assertIn("dwd_ad_order_acc$binlog", REALTIME_JOB)

    def test_attribution_lateness_defaults_to_10_seconds(self):
        self.assertIn('getOrDefault("out-of-orderness-seconds", "10")', CONFIG)
        self.assertIn('getOrDefault("attribution-allowed-lateness-seconds", "10")', CONFIG)
        self.assertIn("forBoundedOutOfOrderness(config.outOfOrderness())", ORDER_JOB)

    def test_no_kafka_runtime_dependency(self):
        self.assertNotIn("kafka", POM.lower())
        compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8").lower()
        self.assertNotIn("kafka", compose)

    def test_sdk_keeps_slot_but_not_classification(self):
        item = SCHEMA["properties"]["events"]["items"]
        self.assertIn("slot_id", item["required"])
        self.assertNotIn("placement_type", item["properties"])
        self.assertNotIn("ad_type", item["properties"])
        self.assertNotIn('"placement_type"', GENERATOR)
        self.assertNotIn('"ad_type"', GENERATOR)

    def test_generic_window_fields_do_not_encode_duration(self):
        body = re.search(
            r"CREATE TABLE IF NOT EXISTS dws_ad_creative_10s \((.*?)\n\) ",
            BOOTSTRAP,
            re.S,
        ).group(1)
        self.assertIn("window_start", body)
        self.assertIn("window_end", body)
        self.assertNotRegex(body, r"window_(?:start|end)_30s|metric_30s")

    def test_dwd_catalog_contains_event_bill_and_order_facts(self):
        created = set(re.findall(r"CREATE TABLE IF NOT EXISTS (dwd_[a-z0-9_]+)", BOOTSTRAP))
        self.assertEqual(created, {"dwd_ad_event_di", "dwd_ad_bill_di", "dwd_ad_order_acc"})

        event = re.search(r"CREATE TABLE IF NOT EXISTS dwd_ad_event_di \((.*?)\n\)", BOOTSTRAP, re.S).group(1)
        bill = re.search(r"CREATE TABLE IF NOT EXISTS dwd_ad_bill_di \((.*?)\n\)", BOOTSTRAP, re.S).group(1)
        order = re.search(r"CREATE TABLE IF NOT EXISTS dwd_ad_order_acc \((.*?)\n\)", BOOTSTRAP, re.S).group(1)
        self.assertNotIn("event_time", event)
        self.assertNotIn("placement_type", event)
        self.assertNotIn("ad_type", event)
        self.assertIn("cost BIGINT", bill)
        self.assertNotIn("is_closed", bill)
        self.assertNotIn("slot_id", order)
        self.assertIn("PRIMARY KEY(order_id)", order)

    def test_pay_refund_and_closed_cost_come_from_acc_changelog_and_unit_dim(self):
        self.assertIn("dwd_ad_order_acc$binlog", REALTIME_JOB)
        self.assertIn("row_before.pay_time IS NULL", REALTIME_JOB)
        self.assertIn("row_before.refund_time IS NULL", REALTIME_JOB)
        self.assertIn("c.is_closed", BILL_MAPPER)
        self.assertIn("ods_mysql_bill_detail", BILL_MAPPER)
        self.assertIn("dwd_ad_bill_di", DWS_SQL)

    def test_bill_cdc_writes_dwd_directly_without_bill_ods(self):
        self.assertIn("'table-name'='bill_info'", CDC_SQL)
        self.assertIn("INSERT INTO fluss.ad_dw.dwd_ad_bill_di", CDC_SQL)
        self.assertIn("UNIX_TIMESTAMP(bill_time)*1000", CDC_SQL)
        legacy_bill_ods = "ods_mysql_" + "bill"
        self.assertNotIn(legacy_bill_ods, BOOTSTRAP + CDC_SQL + DWS_SQL + REALTIME_JOB)

    def test_daily_dws_is_paimon_batch_by_business_time(self):
        self.assertIn("execution.runtime-mode'='batch", DWS_SQL)
        self.assertIn("CREATE CATALOG paimon", DWS_SQL)
        self.assertNotIn("fluss.ad_dw", DWS_SQL)
        self.assertIn("INSERT OVERWRITE paimon.ad_dw.dws_ad_creative_di", DWS_SQL)
        self.assertIn("REPLACE(SUBSTRING(o.pay_time,1,10),'-','')='__BIZ_DATE_COMPACT__'", DWS_SQL)
        self.assertIn("REPLACE(SUBSTRING(o.refund_time,1,10),'-','')='__BIZ_DATE_COMPACT__'", DWS_SQL)
        self.assertNotIn("$binlog", DWS_SQL)
        self.assertIn("u.is_closed=1", DWS_SQL)


if __name__ == "__main__":
    unittest.main()
