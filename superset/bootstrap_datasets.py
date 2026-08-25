from superset.app import create_app


CLASSIFIED_ORDER_METRICS = [
    f"{delivery_type}_{metric}"
    for delivery_type in ("ecommerce", "short_video", "live")
    for metric in (
        "pay_order_count", "refund_order_count",
        "pay_order_gmv", "refund_order_gmv",
    )
]

ADDITIVE_METRICS = [
    "delivery_count", "impression_count", "click_count", "conversion_count",
    "cost", "closed_cost", "pay_order_count", "refund_order_count",
    "pay_order_gmv", "refund_order_gmv",
] + CLASSIFIED_ORDER_METRICS


def topic_columns(entity):
    return [
        ("dt", "DATE", True, True, True),
        (f"{entity}_id", "BIGINT", False, True, True),
        (f"{entity}_name", "VARCHAR", False, True, True),
    ] + [(name, "BIGINT", False, False, False) for name in ADDITIVE_METRICS]


TOPIC_METRICS = (
    [("count", "COUNT(*)")]
    + [(f"total_{name}", f"SUM({name})") for name in ADDITIVE_METRICS]
    + [
        ("ctr", "SUM(click_count)/NULLIF(SUM(impression_count),0)"),
        ("cvr", "SUM(conversion_count)/NULLIF(SUM(click_count),0)"),
        ("roas", "SUM(pay_order_gmv)/NULLIF(SUM(closed_cost),0)"),
    ]
)


DATASETS = {
    f"v_dws_{entity}_di": {
        "main_dttm_col": "dt",
        "columns": topic_columns(entity),
        "metrics": TOPIC_METRICS,
    }
    for entity in ("advertiser", "unit", "creative")
}

DATASETS.update({
    "v_realtime_metric": {
        "main_dttm_col": "window_start",
        "columns": [
            ("window_start", "DATETIME", True, True, True),
            ("window_end", "DATETIME", True, True, True),
            ("dt", "DATE", True, True, True),
            ("advertiser_id", "BIGINT", False, True, True),
            ("campaign_id", "BIGINT", False, True, True),
            ("unit_id", "BIGINT", False, True, True),
            ("creative_id", "BIGINT", False, True, True),
        ] + [(name, "BIGINT", False, False, False) for name in ADDITIVE_METRICS],
        "metrics": TOPIC_METRICS,
    },
    "v_offline_metric": {
        "main_dttm_col": "dt",
        "columns": [
            ("dt", "DATE", True, True, True),
            ("advertiser_id", "BIGINT", False, True, True),
            ("campaign_id", "BIGINT", False, True, True),
            ("unit_id", "BIGINT", False, True, True),
            ("creative_id", "BIGINT", False, True, True),
        ] + [(name, "BIGINT", False, False, False) for name in ADDITIVE_METRICS],
        "metrics": TOPIC_METRICS,
    },
    "v_order_attribution": {
        "main_dttm_col": "dt",
        "columns": [
            ("dt", "DATE", True, True, True), ("order_id", "BIGINT", False, True, True),
            ("uid", "BIGINT", False, True, True), ("product_id", "BIGINT", False, True, True),
            ("pay_time", "DATETIME", True, True, True), ("pay_order_gmv", "BIGINT", False, False, False),
            ("last_click_time", "DATETIME", True, True, True),
            ("advertiser_id", "BIGINT", False, True, True), ("campaign_id", "BIGINT", False, True, True),
            ("unit_id", "BIGINT", False, True, True), ("creative_id", "BIGINT", False, True, True),
            ("attribute_period", "VARCHAR", False, True, True),
        ],
        "metrics": [
            ("pay_order_count", "COUNT(DISTINCT order_id)"),
            ("pay_order_gmv", "SUM(pay_order_gmv)"),
        ],
    },
    "v_ads_advertiser_retention_di": {
        "main_dttm_col": "dt",
        "columns": [
            ("dt", "DATE", True, True, True),
            ("advertiser_count", "BIGINT", False, False, False),
            ("retention_rate_1d", "DOUBLE", False, False, False),
            ("retention_rate_7d", "DOUBLE", False, False, False),
            ("retention_rate_15d", "DOUBLE", False, False, False),
            ("retention_rate_30d", "DOUBLE", False, False, False),
        ],
        "metrics": [("count", "COUNT(*)")],
    },
})


def ensure_dataset(database, table_name, spec):
    from superset import db
    from superset.connectors.sqla.models import SqlaTable, SqlMetric, TableColumn
    dataset = db.session.query(SqlaTable).filter_by(
        database_id=database.id, schema="ad_ads", table_name=table_name
    ).one_or_none()
    if dataset is None:
        dataset = SqlaTable(database_id=database.id, schema="ad_ads", table_name=table_name)
        db.session.add(dataset)
        db.session.flush()
    dataset.main_dttm_col = spec["main_dttm_col"]
    # Update metadata in place so existing chart references and metric IDs stay
    # valid. Only missing columns/metrics are appended.
    columns = {column.column_name: column for column in dataset.columns}
    for name, column_type, is_dttm, groupby, filterable in spec["columns"]:
        column = columns.get(name)
        if column is None:
            column = TableColumn(column_name=name)
            dataset.columns.append(column)
        column.type = column_type
        column.is_dttm = is_dttm
        column.groupby = groupby
        column.filterable = filterable
    metrics = {metric.metric_name: metric for metric in dataset.metrics}
    for name, expression in spec["metrics"]:
        metric = metrics.get(name)
        if metric is None:
            metric = SqlMetric(metric_name=name)
            dataset.metrics.append(metric)
        metric.expression = expression
        metric.metric_type = "sql"


def main():
    app = create_app()
    with app.app_context():
        from superset import db
        from superset.models.core import Database
        database = db.session.query(Database).filter_by(database_name="StarRocks").one()
        for table_name, spec in DATASETS.items():
            ensure_dataset(database, table_name, spec)
        db.session.commit()
        print(f"Bootstrapped {len(DATASETS)} current DWS/ADS datasets.")


if __name__ == "__main__":
    main()
