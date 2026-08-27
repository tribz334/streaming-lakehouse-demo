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
REALTIME_JOB = (ROOT / "flink-java/src/main/java/cn/edu/ustc/lakehouse/realtime/job/RealtimeAdMetricJob.java").read_text(encoding="utf-8")
ATTRIBUTION = (ROOT / "flink-java/src/main/java/cn/edu/ustc/lakehouse/realtime/dwd/LastClickAttributionFunction.java").read_text(encoding="utf-8")
ATTRIBUTION_JOB = (ROOT / "flink-java/src/main/java/cn/edu/ustc/lakehouse/realtime/job/DwdOrderAttributionJob.java").read_text(encoding="utf-8")
GENERATOR = (ROOT / "generator/produce_events.py").read_text(encoding="utf-8")
SCHEMA = json.loads((ROOT / "schemas/ods_log.schema.json").read_text(encoding="utf-8"))


class RuntimeContractTest(unittest.TestCase):
    def test_realtime_table_is_10_seconds_end_to_end(self):
        self.assertIn("CREATE TABLE IF NOT EXISTS ads_realtime_metric_10s", BOOTSTRAP)
        self.assertNotIn("CREATE TABLE IF NOT EXISTS ads_realtime_metric_30s", BOOTSTRAP)
        self.assertIn("ads_realtime_metric_10s", REALTIME_JOB)
        self.assertIn("ads_realtime_metric_10s", STARROCKS)
        self.assertIn("--realtime-metric-window-seconds", SUBMIT_PS)
        self.assertRegex(SUBMIT_PS, r"realtime-metric-window-seconds\s+10")

    def test_window_is_configurable_and_defaults_to_10(self):
        self.assertIn('getOrDefault("realtime-metric-window-seconds", "10")', CONFIG)
        self.assertIn("Time.seconds(config.realtimeMetricWindowSeconds())", REALTIME_JOB)
        self.assertNotRegex(REALTIME_JOB, r"Time\.seconds\(30\)|Duration\.ofSeconds\(30\)")

    def test_last_click_remains_six_hours(self):
        self.assertIn("Duration.ofHours(6)", ATTRIBUTION)
        self.assertNotIn("Duration.ofMinutes(10)", ATTRIBUTION)
        self.assertIn("ValueState<AdClickEvent> latestClickState", ATTRIBUTION)
        self.assertIn("ListState<OrderDetail> pendingOrderState", ATTRIBUTION)
        self.assertNotIn("ListState<AdClickEvent>", ATTRIBUTION)
        self.assertIn(".dwd_ad_order_di", ATTRIBUTION_JOB)
        self.assertIn("order_type='PAY'", ATTRIBUTION_JOB)

    def test_attribution_lateness_defaults_to_10_seconds(self):
        self.assertIn('getOrDefault("out-of-orderness-seconds", "10")', CONFIG)
        self.assertIn('getOrDefault("attribution-allowed-lateness-seconds", "10")', CONFIG)
        self.assertIn("forBoundedOutOfOrderness(config.outOfOrderness())", ATTRIBUTION_JOB)

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
            r"CREATE TABLE IF NOT EXISTS ads_realtime_metric_10s \((.*?)\n\) ",
            BOOTSTRAP,
            re.S,
        ).group(1)
        self.assertIn("window_start", body)
        self.assertIn("window_end", body)
        self.assertNotRegex(body, r"window_(?:start|end)_30s|metric_30s")


if __name__ == "__main__":
    unittest.main()
