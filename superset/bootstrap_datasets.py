from superset.app import create_app


DATASETS = {
    "v_dwd_ad_events_detail": {
        "schema": "ad_ads",
        "main_dttm_col": "event_ts",
        "columns": [
            ("event_date", "VARCHAR", False, True, True),
            ("event_id", "VARCHAR", False, True, True),
            ("event_ts", "DATETIME", True, True, True),
            ("advertiser_id", "VARCHAR", False, True, True),
            ("advertiser_name", "VARCHAR", False, True, True),
            ("industry", "VARCHAR", False, True, True),
            ("tier", "VARCHAR", False, True, True),
            ("campaign_id", "VARCHAR", False, True, True),
            ("campaign_name", "VARCHAR", False, True, True),
            ("unit_id", "VARCHAR", False, True, True),
            ("creative_id", "VARCHAR", False, True, True),
            ("creative_name", "VARCHAR", False, True, True),
            ("media", "VARCHAR", False, True, True),
            ("region", "VARCHAR", False, True, True),
            ("user_id", "VARCHAR", False, True, True),
            ("event_type", "VARCHAR", False, True, True),
            ("spend", "DECIMAL", False, False, False),
            ("gmv", "DECIMAL", False, False, False),
            ("order_id", "VARCHAR", False, True, True),
            ("loaded_at", "DATETIME", True, False, True),
        ],
        "metrics": [
            ("count", "COUNT(*)"),
            ("total_spend", "SUM(spend)"),
            ("total_gmv", "SUM(gmv)"),
            ("impression_events", "SUM(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END)"),
            ("click_events", "SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END)"),
            ("conversion_events", "SUM(CASE WHEN event_type = 'conversion' THEN 1 ELSE 0 END)"),
        ],
    },
    "v_realtime_ad_metrics": {
        "schema": "ad_ads",
        "main_dttm_col": "window_start",
        "columns": [
            ("window_start", "DATETIME", True, True, True),
            ("window_end", "DATETIME", True, True, True),
            ("advertiser_id", "VARCHAR", False, True, True),
            ("advertiser_name", "VARCHAR", False, True, True),
            ("campaign_id", "VARCHAR", False, True, True),
            ("unit_id", "VARCHAR", False, True, True),
            ("creative_id", "VARCHAR", False, True, True),
            ("spend", "DECIMAL", False, False, False),
            ("gmv", "DECIMAL", False, False, False),
            ("impressions", "BIGINT", False, False, False),
            ("clicks", "BIGINT", False, False, False),
            ("conversions", "BIGINT", False, False, False),
            ("orders", "BIGINT", False, False, False),
            ("ctr", "DECIMAL", False, False, False),
            ("cvr", "DECIMAL", False, False, False),
            ("roas", "DECIMAL", False, False, False),
            ("previous_spend", "DECIMAL", False, False, False),
            ("previous_gmv", "DECIMAL", False, False, False),
            ("spend_change", "DECIMAL", False, False, False),
            ("gmv_change", "DECIMAL", False, False, False),
            ("spend_change_rate", "DECIMAL", False, False, False),
            ("gmv_change_rate", "DECIMAL", False, False, False),
            ("updated_at", "DATETIME", True, False, True),
        ],
        "metrics": [
            ("count", "COUNT(*)"),
            ("total_spend", "SUM(spend)"),
            ("total_gmv", "SUM(gmv)"),
            ("total_impressions", "SUM(impressions)"),
            ("total_clicks", "SUM(clicks)"),
            ("total_conversions", "SUM(conversions)"),
            ("total_orders", "SUM(orders)"),
            ("ctr", "SUM(clicks) / NULLIF(SUM(impressions), 0)"),
            ("cvr", "SUM(conversions) / NULLIF(SUM(clicks), 0)"),
            ("roas", "SUM(gmv) / NULLIF(SUM(spend), 0)"),
            ("spend_change", "SUM(spend_change)"),
            ("gmv_change", "SUM(gmv_change)"),
        ],
    },
    "v_advertiser_retention": {
        "schema": "ad_ads",
        "main_dttm_col": "cohort_date",
        "columns": [
            ("cohort_date", "DATE", True, True, True),
            ("cohort_size", "BIGINT", False, False, False),
            ("retained_1d", "BIGINT", False, False, False),
            ("retained_7d", "BIGINT", False, False, False),
            ("retained_15d", "BIGINT", False, False, False),
            ("retained_30d", "BIGINT", False, False, False),
            ("rate_1d", "DECIMAL", False, False, False),
            ("rate_7d", "DECIMAL", False, False, False),
            ("rate_15d", "DECIMAL", False, False, False),
            ("rate_30d", "DECIMAL", False, False, False),
            ("updated_at", "DATETIME", True, False, True),
        ],
        "metrics": [
            ("count", "COUNT(*)"),
            ("cohort_size", "SUM(cohort_size)"),
            ("retained_1d", "SUM(retained_1d)"),
            ("retained_7d", "SUM(retained_7d)"),
            ("次日留存率", "AVG(rate_1d)"),
            ("7日留存率", "AVG(rate_7d)"),
            ("15日留存率", "AVG(rate_15d)"),
            ("30日留存率", "AVG(rate_30d)"),
        ],
    },
    "v_attribution_summary": {
        "schema": "ad_ads",
        "main_dttm_col": "event_date",
        "columns": [
            ("event_date", "DATE", True, True, True),
            ("advertiser_id", "VARCHAR", False, True, True),
            ("advertiser_name", "VARCHAR", False, True, True),
            ("campaign_id", "VARCHAR", False, True, True),
            ("campaign_name", "VARCHAR", False, True, True),
            ("attribution_period", "VARCHAR", False, True, True),
            ("conversions", "BIGINT", False, False, False),
            ("orders", "BIGINT", False, False, False),
            ("attributed_gmv", "DECIMAL", False, False, False),
            ("attributed_spend", "DECIMAL", False, False, False),
            ("updated_at", "DATETIME", True, False, True),
        ],
        "metrics": [
            ("count", "COUNT(*)"),
            ("total_conversions", "SUM(conversions)"),
            ("total_orders", "SUM(orders)"),
            ("total_order_gmv", "SUM(attributed_gmv)"),
            ("attributed_gmv", "SUM(CASE WHEN attribution_period <> '自然订单' THEN attributed_gmv ELSE 0 END)"),
            ("direct_gmv", "SUM(CASE WHEN attribution_period = '30分钟直接归因' THEN attributed_gmv ELSE 0 END)"),
            ("indirect_gmv", "SUM(CASE WHEN attribution_period LIKE '%间接归因' THEN attributed_gmv ELSE 0 END)"),
            ("organic_gmv", "SUM(CASE WHEN attribution_period = '自然订单' THEN attributed_gmv ELSE 0 END)"),
            ("attributed_orders", "SUM(CASE WHEN attribution_period <> '自然订单' THEN orders ELSE 0 END)"),
            ("attributed_order_rate", "SUM(CASE WHEN attribution_period <> '自然订单' THEN orders ELSE 0 END) / NULLIF(SUM(orders), 0)"),
            ("attributed_gmv_rate", "SUM(CASE WHEN attribution_period <> '自然订单' THEN attributed_gmv ELSE 0 END) / NULLIF(SUM(attributed_gmv), 0)"),
            ("attributed_spend", "SUM(attributed_spend)"),
        ],
    },
    "v_order_attribution_detail": {
        "schema": "ad_ads",
        "main_dttm_col": "order_ts",
        "columns": [
            ("event_date", "VARCHAR", False, True, True),
            ("order_event_id", "VARCHAR", False, True, True),
            ("order_id", "VARCHAR", False, True, True),
            ("order_ts", "DATETIME", True, True, True),
            ("user_id", "VARCHAR", False, True, True),
            ("order_advertiser_id", "VARCHAR", False, True, True),
            ("order_advertiser_name", "VARCHAR", False, True, True),
            ("order_campaign_id", "VARCHAR", False, True, True),
            ("order_campaign_name", "VARCHAR", False, True, True),
            ("order_gmv", "DECIMAL", False, False, False),
            ("click_event_id", "VARCHAR", False, True, True),
            ("click_ts", "DATETIME", True, True, True),
            ("creative_id", "VARCHAR", False, True, True),
            ("campaign_id", "VARCHAR", False, True, True),
            ("campaign_name", "VARCHAR", False, True, True),
            ("advertiser_id", "VARCHAR", False, True, True),
            ("advertiser_name", "VARCHAR", False, True, True),
            ("touch_spend", "DECIMAL", False, False, False),
            ("attribution_model", "VARCHAR", False, True, True),
            ("attribution_type", "VARCHAR", False, True, True),
            ("attribution_period", "VARCHAR", False, True, True),
            ("attribution_sort", "INT", False, True, True),
            ("lag_minutes", "BIGINT", False, False, False),
            ("is_attributed", "BOOLEAN", False, True, True),
            ("updated_at", "DATETIME", True, False, True),
        ],
        "metrics": [
            ("order_count", "COUNT(*)"),
            ("order_gmv", "SUM(order_gmv)"),
            ("avg_lag_minutes", "AVG(lag_minutes)"),
        ],
    },
    "v_creative_offline_metrics": {
        "schema": "ad_ads",
        "main_dttm_col": "stat_date",
        "columns": [
            ("stat_date", "DATE", True, True, True),
            ("creative_id", "VARCHAR", False, True, True),
            ("creative_name", "VARCHAR", False, True, True),
            ("creative_format", "VARCHAR", False, True, True),
            ("campaign_id", "VARCHAR", False, True, True),
            ("campaign_name", "VARCHAR", False, True, True),
            ("campaign_objective", "VARCHAR", False, True, True),
            ("campaign_budget", "DECIMAL", False, False, False),
            ("campaign_status", "VARCHAR", False, True, True),
            ("advertiser_id", "VARCHAR", False, True, True),
            ("advertiser_name", "VARCHAR", False, True, True),
            ("industry", "VARCHAR", False, True, True),
            ("advertiser_tier", "VARCHAR", False, True, True),
            ("unit_id", "VARCHAR", False, True, True),
            ("unit_name", "VARCHAR", False, True, True),
            ("bid_type", "VARCHAR", False, True, True),
            ("bid_amount", "DECIMAL", False, False, False),
            ("impressions", "BIGINT", False, False, False),
            ("clicks", "BIGINT", False, False, False),
            ("conversions", "BIGINT", False, False, False),
            ("orders", "BIGINT", False, False, False),
            ("cost", "DECIMAL", False, False, False),
            ("gmv", "DECIMAL", False, False, False),
            ("ctr", "DECIMAL", False, False, False),
            ("cvr", "DECIMAL", False, False, False),
            ("cpc", "DECIMAL", False, False, False),
            ("cpa", "DECIMAL", False, False, False),
            ("roas", "DECIMAL", False, False, False),
            ("is_latest_partition", "BOOLEAN", False, True, True),
            ("updated_at", "DATETIME", True, False, True),
        ],
        "metrics": [
            ("count", "COUNT(*)"),
            ("row_count", "COUNT(*)"),
            ("active_creatives", "COUNT(DISTINCT creative_id)"),
            ("total_cost", "SUM(cost)"),
            ("total_gmv", "SUM(gmv)"),
            ("total_impressions", "SUM(impressions)"),
            ("total_clicks", "SUM(clicks)"),
            ("total_conversions", "SUM(conversions)"),
            ("total_orders", "SUM(orders)"),
            ("ctr", "SUM(clicks) / NULLIF(SUM(impressions), 0)"),
            ("cvr", "SUM(conversions) / NULLIF(SUM(clicks), 0)"),
            ("roas", "SUM(gmv) / NULLIF(SUM(cost), 0)"),
            ("cpc", "SUM(cost) / NULLIF(SUM(clicks), 0)"),
            ("cpa", "SUM(cost) / NULLIF(SUM(conversions), 0)"),
            ("latest_cost", "SUM(CASE WHEN is_latest_partition THEN cost ELSE 0 END)"),
            ("latest_gmv", "SUM(CASE WHEN is_latest_partition THEN gmv ELSE 0 END)"),
            ("latest_impressions", "SUM(CASE WHEN is_latest_partition THEN impressions ELSE 0 END)"),
            ("latest_clicks", "SUM(CASE WHEN is_latest_partition THEN clicks ELSE 0 END)"),
            ("latest_conversions", "SUM(CASE WHEN is_latest_partition THEN conversions ELSE 0 END)"),
            ("latest_orders", "SUM(CASE WHEN is_latest_partition THEN orders ELSE 0 END)"),
            ("latest_roas", "SUM(CASE WHEN is_latest_partition THEN gmv ELSE 0 END) / NULLIF(SUM(CASE WHEN is_latest_partition THEN cost ELSE 0 END), 0)"),
            ("latest_ctr", "SUM(CASE WHEN is_latest_partition THEN clicks ELSE 0 END) / NULLIF(SUM(CASE WHEN is_latest_partition THEN impressions ELSE 0 END), 0)"),
            ("latest_cvr", "SUM(CASE WHEN is_latest_partition THEN conversions ELSE 0 END) / NULLIF(SUM(CASE WHEN is_latest_partition THEN clicks ELSE 0 END), 0)"),
        ],
    },
    "v_fraud_signal_summary": {
        "schema": "ad_ads",
        "main_dttm_col": "event_date",
        "columns": [
            ("event_date", "DATE", True, True, True),
            ("advertiser_id", "VARCHAR", False, True, True),
            ("advertiser_name", "VARCHAR", False, True, True),
            ("suspicious_users", "BIGINT", False, False, False),
            ("suspicious_windows", "BIGINT", False, False, False),
            ("suspicious_clicks", "BIGINT", False, False, False),
            ("suspicious_spend", "DECIMAL", False, False, False),
            ("avg_risk_score", "DECIMAL", False, False, False),
            ("updated_at", "DATETIME", True, False, True),
        ],
        "metrics": [
            ("count", "COUNT(*)"),
            ("suspicious_users", "SUM(suspicious_users)"),
            ("suspicious_windows", "SUM(suspicious_windows)"),
            ("suspicious_clicks", "SUM(suspicious_clicks)"),
            ("suspicious_spend", "SUM(suspicious_spend)"),
            ("avg_risk_score", "AVG(avg_risk_score)"),
        ],
    },
}


def ensure_dataset(database, table_name, spec):
    from superset import db
    from superset.connectors.sqla.models import SqlaTable, SqlMetric, TableColumn

    dataset = (
        db.session.query(SqlaTable)
        .filter_by(database_id=database.id, schema=spec["schema"], table_name=table_name)
        .one_or_none()
    )
    if dataset is None:
        dataset = SqlaTable(
            database_id=database.id,
            schema=spec["schema"],
            table_name=table_name,
            main_dttm_col=spec["main_dttm_col"],
        )
        db.session.add(dataset)
        db.session.flush()
    else:
        dataset.main_dttm_col = spec["main_dttm_col"]

    dataset.columns = []
    dataset.metrics = []
    db.session.flush()

    for name, column_type, is_dttm, groupby, filterable in spec["columns"]:
        dataset.columns.append(
            TableColumn(
                column_name=name,
                type=column_type,
                is_dttm=is_dttm,
                groupby=groupby,
                filterable=filterable,
            )
        )

    for name, expression in spec["metrics"]:
        dataset.metrics.append(
            SqlMetric(
                metric_name=name,
                expression=expression,
                metric_type="sql",
            )
        )


def main():
    app = create_app()
    with app.app_context():
        from superset import db
        from superset.models.core import Database

        database = db.session.query(Database).filter_by(database_name="StarRocks").one()
        for table_name, spec in DATASETS.items():
            ensure_dataset(database, table_name, spec)
        db.session.commit()
        print(f"Bootstrapped {len(DATASETS)} Superset datasets for StarRocks.")


if __name__ == "__main__":
    main()
