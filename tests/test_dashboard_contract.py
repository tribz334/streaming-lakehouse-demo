import ast
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
STARROCKS = (ROOT / "starrocks/init_starrocks.sql").read_text(encoding="utf-8")
DATASETS = (ROOT / "superset/bootstrap_datasets.py").read_text(encoding="utf-8")
DASHBOARD = (ROOT / "superset/bootstrap_dashboard.py").read_text(encoding="utf-8")


class DashboardContractTest(unittest.TestCase):
    def test_bootstrap_python_is_valid(self):
        ast.parse(DATASETS)
        ast.parse(DASHBOARD)

    def test_serving_views_convert_money_to_yuan(self):
        self.assertIn("Serving views expose yuan", STARROCKS)
        for metric in ("cost", "closed_cost", "pay_order_gmv", "short_video_pay_order_gmv", "feed_pay_order_gmv"):
            self.assertRegex(STARROCKS, rf"CAST\((?:SUM\()?{metric}\)? AS DECIMAL\(38,5\)\)/100000 AS {metric}")

    def test_realtime_cards_use_latest_window_dataset(self):
        self.assertIn("CREATE VIEW ad_ads.v_realtime_metric_latest", STARROCKS)
        self.assertIn("SELECT MAX(window_start) AS window_start", STARROCKS)
        self.assertIn('"v_realtime_metric_latest": realtime_dataset()', DATASETS)
        self.assertIn('table_name="v_realtime_metric_latest"', DASHBOARD)

    def test_dashboard_has_no_other_cards(self):
        self.assertNotIn("total_other_", DASHBOARD)
        self.assertNotIn("其他广告", DASHBOARD)

    def test_dashboard_uses_paper_placement_mapping(self):
        self.assertIn('"搜索广告 GMV", "total_search_pay_order_gmv", "placement_type=2"', DASHBOARD)
        self.assertIn('"开屏广告 GMV", "total_splash_pay_order_gmv", "placement_type=3"', DASHBOARD)
        self.assertIn('"信息流广告 GMV", "total_feed_pay_order_gmv", "placement_type=1"', DASHBOARD)

    def test_offline_dashboard_has_required_daily_trends(self):
        self.assertIn('timeseries_chart(offline, "Cost 日趋势", "total_cost"', DASHBOARD)
        self.assertIn('timeseries_chart(offline, "GMV 日趋势", "total_pay_order_gmv"', DASHBOARD)

    def test_offline_dataset_excludes_missing_other_ad_type_column(self):
        self.assertIn("OFFLINE_ADS_CLASSIFIED_METRICS", DATASETS)
        self.assertIn('if metric != "other_ad_type_pay_order_gmv"', DATASETS)
        offline_spec = DATASETS.split('"v_offline_metric": {', 1)[1].split('"v_order_attribution": {', 1)[0]
        self.assertIn("OFFLINE_ADS_CLASSIFIED_METRICS", offline_spec)
        self.assertNotIn("serving_columns(ADS_CLASSIFIED_METRICS)", offline_spec)
        self.assertNotIn("serving_metrics(ADS_CLASSIFIED_METRICS)", offline_spec)


if __name__ == "__main__":
    unittest.main()
