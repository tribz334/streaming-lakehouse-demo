import json
import uuid

from superset.app import create_app


DASHBOARD_TITLE = "广告实时核心指标大盘"
DATASET_NAME = "v_realtime_ad_metrics"


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
            "rolling_type": "None",
            "show_metric_name": False,
            "header_font_size": 0.28,
            "subheader_font_size": 0.12,
            "y_axis_bounds": [None, None],
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
        owner = db.session.query(User).filter_by(username="admin").one()
        dashboard = (
            db.session.query(Dashboard)
            .filter_by(dashboard_title=DASHBOARD_TITLE)
            .one_or_none()
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

        specs = []
        specs.extend([
            metric_card(datasource, "实时 Cost", "total_spend", "实时广告消耗"),
            metric_card(datasource, "实时广告 GMV", "total_gmv", "实时广告 GMV"),
            metric_card(datasource, "实时曝光", "total_impressions", "实时曝光次数"),
            metric_card(datasource, "实时点击", "total_clicks", "实时点击次数"),
            metric_card(datasource, "实时转化", "total_conversions", "实时转化次数"),
            metric_card(datasource, "实时 ROAS", "roas", "广告 GMV / Cost", "SMART_NUMBER"),
        ])
        specs.append({
            "slice_name": "实时 Cost & GMV",
            "viz_type": "echarts_timeseries_line",
            "params": {
                "datasource": "",
                "viz_type": "echarts_timeseries_line",
                "x_axis": "window_start",
                "time_grain_sqla": "P1D",
                "metrics": ["total_gmv", "total_spend"],
                "groupby": [],
                "adhoc_filters": [],
                "row_limit": 10000,
                "show_legend": True,
                "legendOrientation": "top",
                "rich_tooltip": True,
                "y_axis_format": "SMART_NUMBER",
                "color_scheme": "supersetColors",
            },
        })
        specs.append({
            "slice_name": "广告主 GMV 排名",
            "viz_type": "dist_bar",
            "params": {
                "datasource": "",
                "viz_type": "dist_bar",
                "metrics": ["total_gmv"],
                "groupby": ["advertiser_name"],
                "columns": [],
                "adhoc_filters": [],
                "row_limit": 20,
                "order_desc": True,
                "show_legend": False,
                "y_axis_format": "SMART_NUMBER",
            },
        })
        specs.append({
            "slice_name": "CTR / CVR / ROAS",
            "viz_type": "table",
            "params": {
                "datasource": "",
                "viz_type": "table",
                "groupby": ["advertiser_name"],
                "metrics": ["ctr", "cvr", "roas", "total_spend", "total_gmv"],
                "adhoc_filters": [],
                "row_limit": 100,
                "order_by_cols": [["roas", False]],
                "include_search": True,
                "page_length": 20,
            },
        })

        charts = [create_chart(db, Slice, datasource, spec) for spec in specs]
        layout = {}
        root = "ROOT_ID"
        grid = "GRID_ID"
        rows = []
        layout[root] = {"id": root, "type": "ROOT", "children": [grid]}
        layout[grid] = {"id": grid, "type": "GRID", "parents": [root], "children": []}

        row_id = "ROW_KPI"
        rows.append(row_id)
        layout[row_id] = {
            "id": row_id,
            "type": "ROW",
            "parents": [root, grid],
            "children": [],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        for index, chart in enumerate(charts[:6]):
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
                    "width": 2,
                    "height": 25,
                },
            }
            layout[row_id]["children"].append(node)

        for position, chart in enumerate(charts[6:]):
            row_id = f"ROW_DATA_{position}"
            rows.append(row_id)
            layout[row_id] = {
                "id": row_id,
                "type": "ROW",
                "parents": [root, grid],
                "children": [],
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            }
            node = f"CHART-{chart.id}"
            width = 8 if position == 0 else 4
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
                    "height": 50,
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
        dashboard.description = "论文程序化广告实时核心指标监控大盘"
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
