"""Build a complete-day creative snapshot when the Paimon batch is unavailable.

The normal path remains Flink/Paimon -> sync-starrocks-olap.ps1.  This fallback
uses the already synchronized 10-second facts plus MySQL dimensions so the BI
demo still has a correct daily creative-grain serving dataset when Flink batch
slots are temporarily unavailable.
"""

from datetime import datetime
from decimal import Decimal
import os

import MySQLdb


ZERO = Decimal("0")


def ratio(numerator, denominator, scale=6):
    if denominator in (None, ZERO):
        return None
    return (Decimal(numerator or ZERO) / Decimal(denominator)).quantize(
        Decimal(1).scaleb(-scale)
    )


def main():
    mysql = MySQLdb.connect(
        host=os.getenv("MYSQL_HOST", "mysql"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "root"),
        passwd=os.getenv("MYSQL_PASSWORD", "root"),
        db=os.getenv("MYSQL_DATABASE", "ad_ods"),
        charset="utf8mb4",
    )
    starrocks = MySQLdb.connect(
        host=os.getenv("STARROCKS_HOST", "starrocks"),
        port=int(os.getenv("STARROCKS_PORT", "9030")),
        user=os.getenv("STARROCKS_USER", "root"),
        passwd=os.getenv("STARROCKS_PASSWORD", ""),
        db=os.getenv("STARROCKS_DATABASE", "ad_ads"),
        charset="utf8mb4",
    )

    try:
        dimension_cursor = mysql.cursor(MySQLdb.cursors.DictCursor)
        dimension_cursor.execute(
            """
            SELECT
              cr.creative_id, cr.creative_name, cr.format AS creative_format,
              c.campaign_id, c.campaign_name,
              CASE WHEN c.objective = 'ROI' THEN 'ROAS' ELSE c.objective END
                AS campaign_objective,
              c.budget AS campaign_budget, c.status AS campaign_status,
              a.advertiser_id, a.advertiser_name, a.industry,
              a.tier AS advertiser_tier,
              u.unit_id, u.unit_name, u.bid_type, u.bid_amount
            FROM creative cr
            JOIN campaign c ON cr.campaign_id = c.campaign_id
            JOIN advertiser a ON c.advertiser_id = a.advertiser_id
            JOIN `unit` u ON cr.unit_id = u.unit_id
            """
        )
        dimensions = {
            row["creative_id"]: row for row in dimension_cursor.fetchall()
        }

        cursor = starrocks.cursor(MySQLdb.cursors.DictCursor)
        force = os.getenv("FORCE_CREATIVE_OFFLINE_BACKFILL", "false").lower() == "true"
        try:
            cursor.execute("SHOW COLUMNS FROM creative_offline_snapshot LIKE 'roas'")
            has_roas = cursor.fetchone() is not None
            cursor.execute("SELECT COUNT(*) AS row_count FROM creative_offline_snapshot")
            existing_rows = cursor.fetchone()["row_count"]
        except MySQLdb.Error:
            has_roas = False
            existing_rows = 0
        if existing_rows > 0 and has_roas and not force:
            print(
                "Skipped creative offline backfill: "
                f"snapshot already contains {existing_rows} rows."
            )
            return

        cursor.execute(
            """
            SELECT
              DATE(window_start) AS stat_date,
              creative_id,
              MAX(campaign_id) AS campaign_id,
              MAX(unit_id) AS unit_id,
              MAX(advertiser_id) AS advertiser_id,
              MAX(advertiser_name) AS advertiser_name,
              SUM(impressions) AS impressions,
              SUM(clicks) AS clicks,
              SUM(conversions) AS conversions,
              SUM(orders) AS orders,
              SUM(spend) AS cost,
              SUM(gmv) AS gmv
            FROM realtime_ad_metrics_10s
            WHERE window_start < CURRENT_DATE()
            GROUP BY DATE(window_start), creative_id
            ORDER BY stat_date, creative_id
            """
        )
        facts = cursor.fetchall()
        if not facts:
            print("Skipped creative offline backfill: no completed-day realtime facts.")
            return

        cursor.execute("DROP VIEW IF EXISTS v_creative_offline_metrics")
        cursor.execute("DROP TABLE IF EXISTS creative_offline_snapshot")
        cursor.execute(
            """
            CREATE TABLE creative_offline_snapshot (
              stat_date VARCHAR(32) NOT NULL,
              creative_id VARCHAR(64) NOT NULL,
              creative_name VARCHAR(255),
              creative_format VARCHAR(64),
              campaign_id VARCHAR(64),
              campaign_name VARCHAR(255),
              campaign_objective VARCHAR(64),
              campaign_budget DECIMAL(18,2),
              campaign_status VARCHAR(64),
              advertiser_id VARCHAR(64),
              advertiser_name VARCHAR(255),
              industry VARCHAR(128),
              advertiser_tier VARCHAR(64),
              unit_id VARCHAR(64),
              unit_name VARCHAR(255),
              bid_type VARCHAR(64),
              bid_amount DECIMAL(18,4),
              impressions BIGINT,
              clicks BIGINT,
              conversions BIGINT,
              orders BIGINT,
              cost DECIMAL(18,4),
              gmv DECIMAL(18,2),
              ctr DECIMAL(18,6),
              cvr DECIMAL(18,6),
              cpc DECIMAL(18,4),
              cpa DECIMAL(18,4),
              roas DECIMAL(18,6),
              updated_at DATETIME
            )
            DUPLICATE KEY(stat_date, creative_id)
            DISTRIBUTED BY HASH(creative_id) BUCKETS 4
            PROPERTIES ("replication_num" = "1")
            """
        )

        insert_sql = """
            INSERT INTO creative_offline_snapshot VALUES (
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """
        now = datetime.now()
        rows = []
        for fact in facts:
            dim = dimensions.get(fact["creative_id"], {})
            impressions = fact["impressions"] or 0
            clicks = fact["clicks"] or 0
            conversions = fact["conversions"] or 0
            cost = Decimal(fact["cost"] or ZERO)
            gmv = Decimal(fact["gmv"] or ZERO)
            rows.append(
                (
                    fact["stat_date"].isoformat(),
                    fact["creative_id"],
                    dim.get("creative_name", fact["creative_id"]),
                    dim.get("creative_format", "unknown"),
                    dim.get("campaign_id", fact["campaign_id"]),
                    dim.get("campaign_name", fact["campaign_id"]),
                    dim.get("campaign_objective", "UNKNOWN"),
                    dim.get("campaign_budget"),
                    dim.get("campaign_status", "UNKNOWN"),
                    dim.get("advertiser_id", fact["advertiser_id"]),
                    dim.get("advertiser_name", fact["advertiser_name"]),
                    dim.get("industry", "UNKNOWN"),
                    dim.get("advertiser_tier", "UNKNOWN"),
                    dim.get("unit_id", fact["unit_id"]),
                    dim.get("unit_name", fact["unit_id"]),
                    dim.get("bid_type", "UNKNOWN"),
                    dim.get("bid_amount"),
                    impressions,
                    clicks,
                    conversions,
                    fact["orders"] or 0,
                    cost,
                    gmv,
                    ratio(clicks, impressions),
                    ratio(conversions, clicks),
                    ratio(cost, clicks, 4),
                    ratio(cost, conversions, 4),
                    ratio(gmv, cost),
                    now,
                )
            )
        cursor.executemany(insert_sql, rows)
        cursor.execute(
            """
            CREATE VIEW v_creative_offline_metrics AS
            SELECT
              CAST(stat_date AS DATE) AS stat_date,
              creative_id, creative_name, creative_format,
              campaign_id, campaign_name, campaign_objective,
              campaign_budget, campaign_status,
              advertiser_id, advertiser_name, industry, advertiser_tier,
              unit_id, unit_name, bid_type, bid_amount,
              impressions, clicks, conversions, orders, cost, gmv,
              ctr, cvr, cpc, cpa, roas,
              stat_date = (SELECT MAX(stat_date) FROM creative_offline_snapshot)
                AS is_latest_partition,
              updated_at
            FROM creative_offline_snapshot
            """
        )
        starrocks.commit()
        print(
            "Backfilled creative offline snapshot: "
            f"rows={len(rows)}, dates={min(row['stat_date'] for row in facts)}.."
            f"{max(row['stat_date'] for row in facts)}, dimensions={len(dimensions)}."
        )
    finally:
        mysql.close()
        starrocks.close()


if __name__ == "__main__":
    main()
