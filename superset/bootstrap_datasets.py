from superset.app import create_app

BASE_METRICS = [
    "delivery_count", "impression_count", "click_count", "conversion_count",
    "cost", "closed_cost", "pay_order_count", "refund_order_count",
    "pay_order_gmv", "refund_order_gmv",
]
CLASSIFIED_ORDER_METRICS = [
    f"{category}_{metric}"
    for category in (
        "short_video", "live", "image_text", "other_ad_type",
        "search", "splash", "feed", "rewarded", "banner", "other_placement",
    )
    for metric in ("pay_order_count", "refund_order_count", "pay_order_gmv", "refund_order_gmv")
]
ADS_CLASSIFIED_METRICS = [
    "short_video_pay_order_gmv", "live_pay_order_gmv", "image_text_pay_order_gmv",
    "other_ad_type_pay_order_gmv",
    "search_pay_order_gmv", "splash_pay_order_gmv", "feed_pay_order_gmv",
    "rewarded_pay_order_gmv", "banner_pay_order_gmv", "other_placement_pay_order_gmv",
]


def additive_metrics(entity):
    return BASE_METRICS + (CLASSIFIED_ORDER_METRICS if entity in {"advertiser", "campaign"} else [])


def topic_columns(entity):
    columns = [("dt", "DATE", True, True, True)]
    if entity in {"advertiser", "campaign", "unit", "creative"}:
        columns += [(f"{entity}_id", "BIGINT", False, True, True), (f"{entity}_name", "VARCHAR", False, True, True)]
    else:
        columns += [(entity, "INT", False, True, True)]
    if entity == "unit":
        columns += [("placement_type", "INT", False, True, True), ("ad_type", "INT", False, True, True)]
    return columns + [(name, "BIGINT", False, False, False) for name in additive_metrics(entity)]


def topic_metrics(entity=None):
    metrics = BASE_METRICS + (CLASSIFIED_ORDER_METRICS if entity in {"advertiser", "campaign"} else [])
    return [("count", "COUNT(*)")] + [(f"total_{m}", f"SUM({m})") for m in metrics] + [
        ("ctr", "SUM(click_count)/NULLIF(SUM(impression_count),0)"),
        ("cvr", "SUM(conversion_count)/NULLIF(SUM(click_count),0)"),
        ("roas", "SUM(pay_order_gmv)/NULLIF(SUM(closed_cost),0)"),
    ]


DATASETS = {
    f"v_dws_{entity}_di": {"main_dttm_col": "dt", "columns": topic_columns(entity), "metrics": topic_metrics(entity)}
    for entity in ("advertiser", "campaign", "unit", "creative")
}
for entity in ("advertiser", "campaign", "unit", "creative"):
    metrics = additive_metrics(entity)
    columns = [("dt", "DATE", True, True, True), (f"{entity}_id", "BIGINT", False, True, True), (f"{entity}_name", "VARCHAR", False, True, True)]
    if entity == "unit":
        columns += [("placement_type", "INT", False, True, True), ("ad_type", "INT", False, True, True)]
    columns += [(f"{metric}_{window}", "BIGINT", False, False, False) for metric in metrics for window in ("1d", "7d", "30d", "lifetime")]
    DATASETS[f"v_dm_{entity}_df"] = {"main_dttm_col": "dt", "columns": columns, "metrics": [("count", "COUNT(*)")]}

ads_columns = [(name, "BIGINT", False, False, False) for name in BASE_METRICS + ADS_CLASSIFIED_METRICS]
ratio_metrics = [
    ("ctr", "SUM(click_count)/NULLIF(SUM(impression_count),0)"),
    ("cvr", "SUM(conversion_count)/NULLIF(SUM(click_count),0)"),
    ("roas", "SUM(pay_order_gmv)/NULLIF(SUM(closed_cost),0)"),
    ("realtime_roas", "SUM(pay_order_gmv)/NULLIF(SUM(closed_cost),0)"),
]
ads_metrics = [("count", "COUNT(*)")] + [(f"total_{m}", f"SUM({m})") for m in BASE_METRICS + ADS_CLASSIFIED_METRICS] + ratio_metrics
DATASETS.update({
    "v_realtime_metric": {
        "main_dttm_col": "window_start",
        "columns": [("window_start", "DATETIME", True, True, True), ("window_end", "DATETIME", True, True, True), ("dt", "DATE", True, True, True)]
        + ads_columns + [("realtime_roas", "DOUBLE", False, False, False)],
        "metrics": ads_metrics,
    },
    "v_offline_metric": {
        "main_dttm_col": "dt",
        "columns": [("dt", "DATE", True, True, True)]
        + ads_columns + [(name, "DOUBLE", False, False, False) for name in ("ctr", "cvr", "roas")],
        "metrics": ads_metrics,
    },
    "v_order_attribution": {
        "main_dttm_col": "dt",
        "columns": [("dt", "DATE", True, True, True), ("order_id", "BIGINT", False, True, True), ("uid", "BIGINT", False, True, True),
            ("product_id", "BIGINT", False, True, True), ("pay_time", "DATETIME", True, True, True), ("pay_order_gmv", "BIGINT", False, False, False),
            ("last_click_time", "DATETIME", True, True, True)] + [(f"{entity}_id", "BIGINT", False, True, True) for entity in ("advertiser", "campaign", "unit", "creative")]
            + [("placement_type", "INT", False, True, True), ("ad_type", "INT", False, True, True), ("attribute_period", "VARCHAR", False, True, True)],
        "metrics": [("pay_order_count", "COUNT(DISTINCT order_id)"), ("pay_order_gmv", "SUM(pay_order_gmv)")],
    },
    "v_ads_advertiser_retention_di": {
        "main_dttm_col": "dt",
        "columns": [("dt", "DATE", True, True, True), ("advertiser_count", "BIGINT", False, False, False)] + [(f"retention_rate_{w}", "DOUBLE", False, False, False) for w in ("1d", "7d", "15d", "30d")],
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
    # Update metadata in place so unchanged chart references and metric IDs stay
    # valid, while obsolete model columns and metrics are removed.
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
    expected_columns = {name for name, *_ in spec["columns"]}
    for name, column in columns.items():
        if name not in expected_columns:
            db.session.delete(column)
    metrics = {metric.metric_name: metric for metric in dataset.metrics}
    for name, expression in spec["metrics"]:
        metric = metrics.get(name)
        if metric is None:
            metric = SqlMetric(metric_name=name)
            dataset.metrics.append(metric)
        metric.expression = expression
        metric.metric_type = "sql"
    expected_metrics = {name for name, _ in spec["metrics"]}
    for name, metric in metrics.items():
        if name not in expected_metrics:
            db.session.delete(metric)


def main():
    app = create_app()
    with app.app_context():
        from superset import db
        from superset.models.core import Database
        database = db.session.query(Database).filter_by(database_name="StarRocks").one()
        for obsolete in ("v_dws_placement_di", "v_dws_ad_type_di", "v_dm_ad_type_df", "v_dm_placement_df"):
            dataset = db.session.query(SqlaTable).filter_by(
                database_id=database.id, schema="ad_ads", table_name=obsolete
            ).one_or_none()
            if dataset is not None:
                db.session.delete(dataset)
        for table_name, spec in DATASETS.items():
            ensure_dataset(database, table_name, spec)
        db.session.commit()
        print(f"Bootstrapped {len(DATASETS)} current DWS/DM/ADS datasets.")


if __name__ == "__main__":
    main()
