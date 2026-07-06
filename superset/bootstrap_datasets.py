from superset.app import create_app


DATASETS = {
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
            ("roi", "DECIMAL", False, False, False),
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
            ("roi", "SUM(gmv) / NULLIF(SUM(spend), 0)"),
        ],
    },
    "v_advertiser_retention": {
        "schema": "ad_ads",
        "main_dttm_col": "updated_at",
        "columns": [
            ("cohort_date", "VARCHAR", False, True, True),
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
        ],
    },
    "v_attribution_summary": {
        "schema": "ad_ads",
        "main_dttm_col": "updated_at",
        "columns": [
            ("event_date", "VARCHAR", False, True, True),
            ("advertiser_id", "VARCHAR", False, True, True),
            ("advertiser_name", "VARCHAR", False, True, True),
            ("campaign_id", "VARCHAR", False, True, True),
            ("campaign_name", "VARCHAR", False, True, True),
            ("attribution_model", "VARCHAR", False, True, True),
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
            ("attributed_gmv", "SUM(attributed_gmv)"),
            ("attributed_spend", "SUM(attributed_spend)"),
        ],
    },
    "v_fraud_signal_summary": {
        "schema": "ad_ads",
        "main_dttm_col": "updated_at",
        "columns": [
            ("event_date", "VARCHAR", False, True, True),
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
    "v_data_quality_result": {
        "schema": "ad_ads",
        "main_dttm_col": "checked_at",
        "columns": [
            ("check_date", "VARCHAR", False, True, True),
            ("rule_code", "VARCHAR", False, True, True),
            ("rule_name", "VARCHAR", False, True, True),
            ("data_layer", "VARCHAR", False, True, True),
            ("target_table", "VARCHAR", False, True, True),
            ("actual_value", "DECIMAL", False, False, False),
            ("expected_value", "VARCHAR", False, True, True),
            ("check_status", "VARCHAR", False, True, True),
            ("severity", "VARCHAR", False, True, True),
            ("details", "VARCHAR", False, False, True),
            ("checked_at", "DATETIME", True, False, True),
        ],
        "metrics": [
            ("total_rules", "COUNT(*)"),
            ("passed_rules", "SUM(CASE WHEN check_status = 'PASS' THEN 1 ELSE 0 END)"),
            ("failed_rules", "SUM(CASE WHEN check_status = 'FAIL' THEN 1 ELSE 0 END)"),
        ],
    },
    "v_data_quality_summary": {
        "schema": "ad_ads",
        "main_dttm_col": "checked_at",
        "columns": [
            ("check_date", "VARCHAR", False, True, True),
            ("total_rules", "BIGINT", False, False, False),
            ("passed_rules", "BIGINT", False, False, False),
            ("failed_rules", "BIGINT", False, False, False),
            ("quality_score", "DECIMAL", False, False, False),
            ("overall_status", "VARCHAR", False, True, True),
            ("checked_at", "DATETIME", True, False, True),
        ],
        "metrics": [
            ("latest_quality_score", "MAX(quality_score)"),
            ("failed_rules", "SUM(failed_rules)"),
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
