import json
import uuid

from superset.app import create_app


DASHBOARD_TITLE = "广告实时核心指标大盘"
LEGACY_DASHBOARD_TITLE = "Real-Time Advertising Performance Dashboard"
DATASET_NAME = "v_realtime_ad_metrics"
TODAY_DATASET_NAME = "v_realtime_ad_metrics_today"
# Superset 3 parses relative ranges in UTC while StarRocks keeps the business
# window as Asia/Shanghai local time. This UTC+8 equivalent keeps both the SQL
# filter and the displayed axis on Beijing time.
TREND_RANGE = "6 hours from now : 8 hours from now"

LEGACY_CHART_NAMES = {
    "广告消耗": "Ad Spend",
    "内循环消耗": "自循环消耗",
    "内循环GMV": "自循环GMV",
    "短视频GMV": "短视频归因成交金额",
    "直播GMV": "直播归因成交金额",
    "电商GMV": "商城归因成交金额",
    "近2小时广告消耗与内循环GMV趋势": "近2小时广告消耗与自循环GMV趋势",
}


def realtime_filter():
    return {
        "expressionType": "SIMPLE",
        "subject": "window_start_local",
        "operator": "TEMPORAL_RANGE",
        "comparator": TREND_RANGE,
        "clause": "WHERE",
        "sqlExpression": None,
    }


def metric_card(datasource, name, metric, subheader, y_axis_format="SMART_NUMBER"):
    return {
        "slice_name": name,
        "viz_type": "big_number_total",
        "params": {
            "datasource": f"{datasource.id}__table",
            "viz_type": "big_number_total",
            "metric": metric,
            "subheader": subheader,
            "y_axis_format": y_axis_format,
            "time_range": "No filter",
            "adhoc_filters": [],
            "rolling_type": "None",
            "show_metric_name": False,
            "header_font_size": 0.28,
            "subheader_font_size": 0.15,
            "y_axis_bounds": [None, None],
        },
    }


def create_chart(db, Slice, datasource, spec):
    chart_names = [spec["slice_name"]]
    legacy_name = LEGACY_CHART_NAMES.get(spec["slice_name"])
    if legacy_name:
        chart_names.append(legacy_name)
    candidates = (
        db.session.query(Slice)
        .filter(Slice.slice_name.in_(chart_names))
        .order_by(Slice.id)
        .all()
    )
    chart = next(
        (candidate for candidate in candidates if candidate.slice_name == spec["slice_name"]),
        candidates[0] if candidates else None,
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
    chart.slice_name = spec["slice_name"]
    chart.datasource_type = "table"
    chart.datasource_id = datasource.id
    chart.datasource_name = f"{datasource.schema}.{datasource.table_name}"
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
            query_columns.append({
                "timeGrain": params["time_grain_sqla"],
                "columnType": "BASE_AXIS",
                "sqlExpression": column,
                "label": column,
                "expressionType": "SQL",
            })
        else:
            query_columns.append(column)
    query = {
        "filters": [],
        "granularity": x_axis or datasource.main_dttm_col or "window_start",
        "extras": {"having": "", "where": ""},
        "applied_time_extras": {},
        "columns": query_columns,
        "metrics": metrics,
        "orderby": [[metrics[0], False]] if metrics else [],
        "annotation_layers": [],
        "row_limit": params.get("row_limit", 10000),
        "series_columns": [],
        "series_limit": 0,
        "order_desc": True,
        "url_params": {},
        "custom_params": {},
        "custom_form_data": {},
        "time_offsets": [],
        "time_range": params.get("time_range", TREND_RANGE),
    }
    chart.query_context = json.dumps({
        "datasource": {"id": datasource.id, "type": "table"},
        "force": False,
        "queries": [query],
        "form_data": params,
        "result_format": "json",
        "result_type": "full",
    }, ensure_ascii=False)
    chart.description = "论文广告实时核心指标大盘自动生成图表"
    return chart


def main():
    app = create_app()
    with app.app_context():
        from superset import db
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice
        from flask_appbuilder.security.sqla.models import User

        datasource = (
            db.session.query(SqlaTable)
            .filter_by(table_name=DATASET_NAME, schema="ad_ads")
            .one()
        )
        today_datasource = (
            db.session.query(SqlaTable)
            .filter_by(table_name=TODAY_DATASET_NAME, schema="ad_ads")
            .one()
        )
        owner = db.session.query(User).filter_by(username="admin").one()
        dashboard = (
            db.session.query(Dashboard)
            .filter(Dashboard.dashboard_title.in_([DASHBOARD_TITLE, LEGACY_DASHBOARD_TITLE]))
            .order_by(Dashboard.id)
            .first()
        )
        if dashboard is None:
            dashboard = Dashboard(
                dashboard_title=DASHBOARD_TITLE,
                slug="ad-realtime-core-metrics",
                published=True,
                created_by_fk=1,
                changed_by_fk=1,
                uuid=uuid.uuid4(),
            )
            db.session.add(dashboard)
            db.session.flush()
        dashboard.dashboard_title = DASHBOARD_TITLE

        # Paper-aligned realtime dashboard: cards are cumulative from the
        # current business-day midnight; the line chart remains window based.
        specs = [
            metric_card(today_datasource, "广告消耗", "total_spend", "今日累计"),
            metric_card(today_datasource, "内循环消耗", "inner_loop_spend", "今日累计"),
            metric_card(today_datasource, "内循环GMV", "inner_loop_gmv", "今日累计"),
            metric_card(today_datasource, "ROAS", "inner_loop_roas", "内循环GMV / 内循环消耗", ",.2f"),
            metric_card(today_datasource, "短视频GMV", "short_video_attributed_gmv", "今日累计"),
            metric_card(today_datasource, "直播GMV", "live_attributed_gmv", "今日累计"),
            metric_card(today_datasource, "电商GMV", "shop_attributed_gmv", "今日累计"),
        ]
        specs.append({
            "slice_name": "近2小时广告消耗与内循环GMV趋势",
            "viz_type": "echarts_timeseries_line",
            "params": {
                "datasource": "",
                "viz_type": "echarts_timeseries_line",
                "x_axis": "window_start_local",
                "time_grain_sqla": "PT1M",
                "time_range": TREND_RANGE,
                "metrics": ["内循环GMV", "广告消耗"],
                "groupby": [],
                "adhoc_filters": [realtime_filter()],
                "row_limit": 10000,
                "show_legend": True,
                "legendOrientation": "top",
                "rich_tooltip": True,
                "zoomable": True,
                "y_axis_format": "SMART_NUMBER",
                "color_scheme": "supersetColors",
            },
        })
        charts = [
            create_chart(db, Slice, today_datasource, spec)
            for spec in specs[:7]
        ]
        charts.append(create_chart(db, Slice, datasource, specs[7]))
        layout = {}
        root = "ROOT_ID"
        grid = "GRID_ID"
        rows = []
        layout[root] = {"id": root, "type": "ROOT", "children": [grid]}
        layout[grid] = {"id": grid, "type": "GRID", "parents": [root], "children": []}

        row_id = "ROW_REFRESH_NOTICE"
        notice_id = "MARKDOWN_REFRESH_NOTICE"
        rows.append(row_id)
        layout[row_id] = {
            "id": row_id,
            "type": "ROW",
            "parents": [root, grid],
            "children": [notice_id],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        layout[notice_id] = {
            "id": notice_id,
            "type": "MARKDOWN",
            "parents": [root, grid, row_id],
            "children": [],
            "meta": {
                "code": "**顶部卡片：今日累计** ｜ **趋势图：10 秒事件时间窗口** ｜ 每 10 秒刷新，约有 5–15 秒处理延迟",
                "width": 12,
                "height": 5,
            },
        }

        row_id = "ROW_KPI_CORE"
        rows.append(row_id)
        layout[row_id] = {
            "id": row_id,
            "type": "ROW",
            "parents": [root, grid],
            "children": [],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        for chart in charts[:4]:
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
                    "width": 3,
                    "height": 24,
                },
            }
            layout[row_id]["children"].append(node)

        row_id = "ROW_KPI_SCENE"
        rows.append(row_id)
        layout[row_id] = {
            "id": row_id,
            "type": "ROW",
            "parents": [root, grid],
            "children": [],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        for chart in charts[4:7]:
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
                    "width": 4,
                    "height": 24,
                },
            }
            layout[row_id]["children"].append(node)

        row_id = "ROW_REALTIME_TREND"
        rows.append(row_id)
        layout[row_id] = {
            "id": row_id,
            "type": "ROW",
            "parents": [root, grid],
            "children": [],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        for chart, width in ((charts[7], 12),):
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
                    "height": 58,
                },
            }
            layout[row_id]["children"].append(node)
        layout[grid]["children"] = rows
        layout["HEADER_ID"] = {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": DASHBOARD_TITLE}}
        layout["DASHBOARD_VERSION_KEY"] = "v2"

        dashboard.position_json = json.dumps(layout, ensure_ascii=False)
        chart_ids = [chart.id for chart in charts]
        dashboard.json_metadata = json.dumps({
            "chart_configuration": {
                str(chart.id): {
                    "id": chart.id,
                    "crossFilters": {"scope": "global", "chartsInScope": []},
                }
                for chart in charts
            },
            "global_chart_configuration": {
                "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                "chartsInScope": chart_ids,
            },
            "color_scheme": "supersetColors",
            "shared_label_colors": {},
            "color_scheme_domain": [],
            "expanded_slices": {},
            "label_colors": {},
            "timed_refresh_immune_slices": [],
            "default_filters": "{}",
            "refresh_frequency": 10,
            "cross_filters_enabled": True,
            "native_filter_configuration": [],
        }, ensure_ascii=False)
        dashboard.published = True
        dashboard.css = ""
        dashboard.description = "程序化广告实时核心指标大盘；每10秒自动刷新，采用事件时间窗口统计。"
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
