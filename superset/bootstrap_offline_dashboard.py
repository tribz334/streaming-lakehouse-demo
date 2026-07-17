import json
import uuid

from superset.app import create_app


DASHBOARD_TITLE = "广告离线核心指标大盘"
DATASET_NAME = "v_creative_offline_metrics"
DEFAULT_TIME_RANGE = "Last 14 days"


def metric_card(datasource, name, metric, subheader, y_axis_format="SMART_NUMBER"):
    return {
        "slice_name": name,
        "viz_type": "big_number_total",
        "params": {
            "viz_type": "big_number_total",
            "metric": metric,
            "subheader": subheader,
            "y_axis_format": y_axis_format,
            "time_range": DEFAULT_TIME_RANGE,
            "rolling_type": "None",
            "show_metric_name": False,
            "header_font_size": 0.28,
            "subheader_font_size": 0.12,
            "y_axis_bounds": [None, None],
        },
    }


def timeseries(name, metrics, y_axis_format="SMART_NUMBER"):
    return {
        "slice_name": name,
        "viz_type": "echarts_timeseries_line",
        "params": {
            "viz_type": "echarts_timeseries_line",
            "x_axis": "stat_date",
            "time_grain_sqla": "P1D",
            "time_range": DEFAULT_TIME_RANGE,
            "metrics": metrics,
            "groupby": [],
            "adhoc_filters": [],
            "row_limit": 10000,
            "show_legend": True,
            "legendOrientation": "top",
            "rich_tooltip": True,
            "markerEnabled": True,
            "markerSize": 4,
            "y_axis_format": y_axis_format,
            "color_scheme": "supersetColors",
        },
    }


def bar_chart(name, metric, groupby, row_limit=15):
    return {
        "slice_name": name,
        "viz_type": "dist_bar",
        "params": {
            "viz_type": "dist_bar",
            "metrics": [metric],
            "groupby": [groupby],
            "columns": [],
            "time_range": DEFAULT_TIME_RANGE,
            "adhoc_filters": [],
            "row_limit": row_limit,
            "order_desc": True,
            "show_legend": False,
            "y_axis_format": "SMART_NUMBER",
            "color_scheme": "supersetColors",
        },
    }


def detail_table():
    return {
        "slice_name": "创意表现下钻明细",
        "viz_type": "table",
        "params": {
            "viz_type": "table",
            "groupby": [
                "stat_date",
                "advertiser_name",
                "industry",
                "campaign_name",
                "campaign_objective",
                "unit_name",
                "creative_name",
                "creative_format",
            ],
            "metrics": [
                "total_cost",
                "total_gmv",
                "total_impressions",
                "total_clicks",
                "total_conversions",
                "total_orders",
                "ctr",
                "cvr",
                "roas",
                "cpc",
                "cpa",
            ],
            "time_range": DEFAULT_TIME_RANGE,
            "adhoc_filters": [],
            "row_limit": 1000,
            "include_search": True,
            "page_length": 25,
            "order_desc": True,
        },
    }


def create_chart(db, Slice, datasource, spec):
    chart = (
        db.session.query(Slice)
        .filter_by(slice_name=spec["slice_name"], datasource_id=datasource.id)
        .one_or_none()
    )
    if chart is None:
        chart = Slice(
            slice_name=spec["slice_name"],
            datasource_type="table",
            datasource_id=datasource.id,
            datasource_name=f"{datasource.schema}.{datasource.table_name}",
            viz_type=spec["viz_type"],
            created_by_fk=1,
            changed_by_fk=1,
            uuid=uuid.uuid4(),
        )
        db.session.add(chart)
        db.session.flush()

    params = dict(spec["params"])
    params["slice_id"] = chart.id
    params["datasource"] = f"{datasource.id}__table"
    params["dashboards"] = []
    chart.viz_type = spec["viz_type"]
    chart.params = json.dumps(params, ensure_ascii=False)

    metrics = params.get("metrics") or ([params["metric"]] if params.get("metric") else [])
    columns = list(params.get("groupby", []))
    x_axis = params.get("x_axis")
    if x_axis and x_axis not in columns:
        columns.insert(0, x_axis)
    query_columns = []
    for column in columns:
        if column == x_axis and params.get("time_grain_sqla"):
            query_columns.append(
                {
                    "timeGrain": params["time_grain_sqla"],
                    "columnType": "BASE_AXIS",
                    "sqlExpression": column,
                    "label": column,
                    "expressionType": "SQL",
                }
            )
        else:
            query_columns.append(column)

    query = {
        "filters": [],
        "extras": {"having": "", "where": ""},
        "applied_time_extras": {},
        "columns": query_columns,
        "metrics": metrics,
        "orderby": [[metrics[0], False]] if metrics else [],
        "annotation_layers": [],
        "row_limit": params.get("row_limit", 10000),
        "series_columns": [],
        "series_limit": 0,
        "order_desc": params.get("order_desc", True),
        "url_params": {},
        "custom_params": {},
        "custom_form_data": {},
        "time_offsets": [],
        "time_range": params.get("time_range", DEFAULT_TIME_RANGE),
    }
    chart.query_context = json.dumps(
        {
            "datasource": {"id": datasource.id, "type": "table"},
            "force": False,
            "queries": [query],
            "form_data": params,
            "result_format": "json",
            "result_type": "full",
        },
        ensure_ascii=False,
    )
    chart.description = "创意粒度离线 ADS 数据集上的多维聚合分析"
    return chart


def add_row(layout, root, grid, row_id, charts, height):
    layout[row_id] = {
        "id": row_id,
        "type": "ROW",
        "parents": [root, grid],
        "children": [],
        "meta": {"background": "BACKGROUND_TRANSPARENT"},
    }
    width = 12 // len(charts)
    for chart in charts:
        node = f"CHART-{chart.id}"
        layout[node] = {
            "id": node,
            "type": "CHART",
            "parents": [root, grid, row_id],
            "children": [],
            "meta": {
                "chartId": chart.id,
                "sliceName": chart.slice_name,
                "uuid": str(chart.uuid),
                "width": width,
                "height": height,
            },
        }
        layout[row_id]["children"].append(node)


def select_filter(filter_id, name, column, datasource_id, root):
    return {
        "id": filter_id,
        "controlValues": {
            "enableEmptyFilter": False,
            "defaultToFirstItem": False,
            "multiSelect": True,
            "searchAllOptions": True,
            "inverseSelection": False,
        },
        "name": name,
        "filterType": "filter_select",
        "targets": [{"column": {"name": column}, "datasetId": datasource_id}],
        "defaultDataMask": {"extraFormData": {}, "filterState": {}},
        "cascadeParentIds": [],
        "scope": {"rootPath": [root], "excluded": []},
        "type": "NATIVE_FILTER",
        "description": "",
    }


def main():
    app = create_app()
    with app.app_context():
        from flask_appbuilder.security.sqla.models import User
        from superset import db
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice

        datasource = (
            db.session.query(SqlaTable)
            .filter_by(table_name=DATASET_NAME, schema="ad_ads")
            .one()
        )
        owner = db.session.query(User).filter_by(username="admin").one()
        dashboard = (
            db.session.query(Dashboard)
            .filter_by(dashboard_title=DASHBOARD_TITLE)
            .one_or_none()
        )
        if dashboard is None:
            dashboard = Dashboard(
                dashboard_title=DASHBOARD_TITLE,
                slug="ad-offline-core-metrics",
                published=True,
                created_by_fk=owner.id,
                changed_by_fk=owner.id,
                uuid=uuid.uuid4(),
            )
            db.session.add(dashboard)
            db.session.flush()

        specs = [
            metric_card(datasource, "昨日 Cost", "latest_cost", "最新完整分区广告消耗"),
            metric_card(datasource, "昨日广告 GMV", "latest_gmv", "最新完整分区成交金额"),
            metric_card(datasource, "昨日曝光", "latest_impressions", "最新完整分区曝光量"),
            metric_card(datasource, "昨日点击", "latest_clicks", "最新完整分区点击量"),
            metric_card(datasource, "昨日转化", "latest_conversions", "最新完整分区转化量"),
            metric_card(datasource, "昨日订单", "latest_orders", "最新完整分区订单量"),
            metric_card(datasource, "昨日 ROAS", "latest_roas", "广告 GMV / Cost", ".2f"),
            metric_card(datasource, "昨日 CTR", "latest_ctr", "点击 / 曝光", ".2%"),
            metric_card(datasource, "昨日 CVR", "latest_cvr", "转化 / 点击", ".2%"),
            timeseries("近两周 Cost 与 GMV 趋势", ["total_cost", "total_gmv"]),
            timeseries(
                "近两周流量与转化趋势",
                ["total_impressions", "total_clicks", "total_conversions", "total_orders"],
            ),
            timeseries("近两周效率趋势", ["roas", "ctr", "cvr"], ".3f"),
            bar_chart("广告主 GMV 贡献排名", "total_gmv", "advertiser_name"),
            bar_chart("投放目标 GMV 结构", "total_gmv", "campaign_objective", row_limit=10),
            bar_chart("创意 GMV 排名", "total_gmv", "creative_name", row_limit=20),
            detail_table(),
        ]
        charts = [create_chart(db, Slice, datasource, spec) for spec in specs]

        root = "ROOT_ID"
        grid = "GRID_ID"
        layout = {
            root: {"id": root, "type": "ROOT", "children": [grid]},
            grid: {"id": grid, "type": "GRID", "parents": [root], "children": []},
            "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": DASHBOARD_TITLE}},
            "DASHBOARD_VERSION_KEY": "v2",
        }
        rows = [
            ("ROW_VOLUME_KPI", charts[0:6], 24),
            ("ROW_EFFICIENCY_KPI", charts[6:9], 24),
            ("ROW_BUSINESS_TREND", charts[9:11], 52),
            ("ROW_EFFICIENCY_TREND", charts[11:12], 44),
            ("ROW_RANKING", charts[12:15], 48),
            ("ROW_CREATIVE_DETAIL", charts[15:16], 72),
        ]
        for row_id, row_charts, height in rows:
            add_row(layout, root, grid, row_id, row_charts, height)
            layout[grid]["children"].append(row_id)

        time_filter = {
            "id": "NATIVE_FILTER-OFFLINE_TIME",
            "name": "统计日期",
            "filterType": "filter_time",
            "targets": [{"datasetId": datasource.id}],
            "defaultDataMask": {
                "extraFormData": {"time_range": DEFAULT_TIME_RANGE},
                "filterState": {"value": DEFAULT_TIME_RANGE},
            },
            "cascadeParentIds": [],
            "scope": {"rootPath": [root], "excluded": []},
            "type": "NATIVE_FILTER",
            "description": "默认查看近 14 天，可自由调整日期范围",
        }
        native_filters = [
            time_filter,
            select_filter("NATIVE_FILTER-ADVERTISER", "广告主", "advertiser_name", datasource.id, root),
            select_filter("NATIVE_FILTER-INDUSTRY", "行业", "industry", datasource.id, root),
            select_filter("NATIVE_FILTER-OBJECTIVE", "投放目标", "campaign_objective", datasource.id, root),
            select_filter("NATIVE_FILTER-FORMAT", "创意形式", "creative_format", datasource.id, root),
        ]

        chart_ids = [chart.id for chart in charts]
        dashboard.position_json = json.dumps(layout, ensure_ascii=False)
        dashboard.json_metadata = json.dumps(
            {
                "chart_configuration": {
                    str(chart.id): {
                        "id": chart.id,
                        "crossFilters": {"scope": "global", "chartsInScope": []},
                    }
                    for chart in charts
                },
                "global_chart_configuration": {
                    "scope": {"rootPath": [root], "excluded": []},
                    "chartsInScope": chart_ids,
                },
                "color_scheme": "supersetColors",
                "shared_label_colors": {},
                "color_scheme_domain": [],
                "expanded_slices": {},
                "label_colors": {},
                "timed_refresh_immune_slices": [],
                "default_filters": "{}",
                "refresh_frequency": 0,
                "cross_filters_enabled": True,
                "native_filter_configuration": native_filters,
            },
            ensure_ascii=False,
        )
        dashboard.published = True
        dashboard.description = (
            "基于创意粒度离线 ADS 数据集的昨日核心指标、近两周趋势、"
            "贡献排名与广告主/计划/创意下钻分析。"
        )
        dashboard.created_by_fk = owner.id
        dashboard.changed_by_fk = owner.id
        dashboard.owners = [owner]
        dashboard.slices = charts
        for chart in charts:
            chart.created_by_fk = owner.id
            chart.changed_by_fk = owner.id
            chart.owners = [owner]
        db.session.commit()
        print(f"Bootstrapped dashboard '{DASHBOARD_TITLE}' with {len(charts)} charts.")


if __name__ == "__main__":
    main()
