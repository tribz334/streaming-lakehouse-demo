import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DAILY = (ROOT / "flink/sql/10_daily_offline.sql").read_text(encoding="utf-8")
DWS = (ROOT / "flink/sql/04_daily_dws.sql").read_text(encoding="utf-8")
INITIALIZE = (ROOT / "flink/sql/11_initialize_dm.sql").read_text(encoding="utf-8")
RUN_PS = (ROOT / "scripts/windows/run-daily-batch.ps1").read_text(encoding="utf-8")
RUN_SH = (ROOT / "scripts/linux/run-daily-batch.sh").read_text(encoding="utf-8")
MIGRATION = (ROOT / "mysql/migrations/039_align_demo_semantics.sql").read_text(encoding="utf-8")
CLASSIFICATION_MIGRATION = (ROOT / "mysql/migrations/038_unit_classification.sql").read_text(encoding="utf-8")
MIGRATE_PS = (ROOT / "scripts/windows/apply-mysql-migrations.ps1").read_text(encoding="utf-8")
MIGRATE_SH = (ROOT / "scripts/linux/apply-mysql-migrations.sh").read_text(encoding="utf-8")


class DailyDmRollingContractTest(unittest.TestCase):
    def test_daily_dws_precedes_ads_and_dm_in_batch_launchers(self):
        self.assertIn("SET 'execution.runtime-mode'='batch'", DWS)
        self.assertIn("flink\\sql\\04_daily_dws.sql", RUN_PS)
        self.assertIn("flink/sql/04_daily_dws.sql", RUN_SH)

    def test_daily_dm_reads_previous_snapshot_for_all_topics(self):
        for topic in ("advertiser", "campaign", "unit", "creative"):
            self.assertRegex(
                DAILY,
                rf"LEFT JOIN paimon\.ad_dw\.dm_ad_{topic}_df prev\s+"
                rf"ON dim\.{topic}_id=prev\.{topic}_id AND prev\.dt='__PREV_DATE__'",
            )

    def test_rolling_formulas_add_today_and_remove_expired_partition(self):
        for metric in (
            "delivery_count",
            "impression_count",
            "click_count",
            "conversion_count",
            "cost",
            "closed_cost",
            "pay_order_count",
            "refund_order_count",
            "pay_order_gmv",
            "refund_order_gmv",
        ):
            self.assertIn(f"prev.{metric}_7d", DAILY)
            self.assertIn(f"expired7.{metric}", DAILY)
            self.assertIn(f"prev.{metric}_30d", DAILY)
            self.assertIn(f"expired30.{metric}", DAILY)
            self.assertIn(f"prev.{metric}_lifetime", DAILY)

    def test_initialization_remains_history_based(self):
        self.assertNotIn("__PREV_DATE__", INITIALIZE)
        self.assertIn("__DATE_MINUS_6__", INITIALIZE)
        self.assertIn("__DATE_MINUS_29__", INITIALIZE)

    def test_launchers_render_all_daily_rolling_dates(self):
        for script in (RUN_PS, RUN_SH):
            self.assertIn("__PREV_DATE__", script)
            self.assertIn("__DATE_MINUS_7__", script)
            self.assertIn("__DATE_MINUS_30__", script)

    def test_ads_attribution_preserves_direct_and_backfills_last_click(self):
        self.assertIn("WHEN direct_creative_id IS NOT NULL THEN 'DIRECT'", DAILY)
        self.assertIn("WHEN candidate_click_time >= pay_ts-INTERVAL '1' DAY THEN '1D'", DAILY)
        self.assertIn("WHEN candidate_click_time >= pay_ts-INTERVAL '7' DAY THEN '7D'", DAILY)
        self.assertIn("WHEN candidate_click_time >= pay_ts-INTERVAL '30' DAY THEN '30D'", DAILY)
        self.assertIn("ELSE 'ORGANIC'", DAILY)
        self.assertIn("ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY c.ts DESC,c.event_id DESC)", DAILY)
        self.assertIn("o.uid=c.uid", DAILY)
        self.assertIn("o.product_id=c.product_id", DAILY)
        self.assertIn("c.event_type='click'", DAILY)
        self.assertNotIn("o.click_time", DAILY)
        self.assertIn("TO_TIMESTAMP_LTZ(c.ts,3)<=TO_TIMESTAMP_LTZ", DAILY)
        self.assertIn("INTERVAL '30' DAY", DAILY)

    def test_placement_migration_is_marked_and_permuted_once(self):
        self.assertIn("demo_schema_migrations", MIGRATION)
        self.assertIn("039_placement_type_paper_mapping", MIGRATION)
        self.assertRegex(MIGRATION, r"WHEN 1 THEN 2\s+WHEN 2 THEN 3\s+WHEN 3 THEN 1")
        self.assertIn("1-feed,2-search,3-splash", MIGRATION)
        self.assertIn("column_comment LIKE '%1-feed,2-search,3-splash%'", MIGRATION)
        self.assertIn("QUOTE(@placement_comment_038)", CLASSIFICATION_MIGRATION)

    def test_user_seed_covers_sdk_and_fraud_namespaces(self):
        self.assertIn("n.seq BETWEEN 1 AND 12000", MIGRATION)
        self.assertIn("10000000+n.seq", MIGRATION)
        for uid in (90000001, 90000002, 90000003):
            self.assertIn(str(uid), MIGRATION)

    def test_both_migration_launchers_include_naming_and_semantic_migrations(self):
        for script in (MIGRATE_PS, MIGRATE_SH):
            self.assertIn("mysql/migrations/028_unify_fact_table_names.sql", script)
            self.assertIn("mysql/migrations/039_align_demo_semantics.sql", script)


if __name__ == "__main__":
    unittest.main()
