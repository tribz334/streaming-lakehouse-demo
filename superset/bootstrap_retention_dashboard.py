import json
import uuid

from superset.app import create_app


DASHBOARD_TITLE = "广告主留存分析"
DATASET_NAME = "v_advertiser_retention"
CHART_NAME = "广告主留存率走势"


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
                slug="advertiser-retention-analysis",
                published=True,
                created_by_fk=owner.id,
                changed_by_fk=owner.id,
                uuid=uuid.uuid4(),
            )
            db.session.add(dashboard)
            db.session.flush()

        chart = (
            db.session.query(Slice)
            .filter_by(slice_name=CHART_NAME, datasource_id=datasource.id)
            .one_or_none()
        )
        if chart is None:
            chart = Slice(
                slice_name=CHART_NAME,
                datasource_type="table",
                datasource_id=datasource.id,
                datasource_name=f"{datasource.schema}.{datasource.table_name}",
                viz_type="echarts_timeseries_line",
                created_by_fk=owner.id,
                changed_by_fk=owner.id,
                uuid=uuid.uuid4(),
            )
            db.session.add(chart)
            db.session.flush()

        metrics = ["次日留存率", "7日留存率", "15日留存率", "30日留存率"]
        params = {
            "datasource": f"{datasource.id}__table",
            "slice_id": chart.id,
            "dashboards": [],
            "viz_type": "echarts_timeseries_line",
            "x_axis": "cohort_date",
            "time_grain_sqla": "P1D",
            "time_range": "No filter",
            "metrics": metrics,
            "groupby": [],
            "adhoc_filters": [],
            "row_limit": 10000,
            "show_legend": True,
            "legendOrientation": "top",
            "rich_tooltip": True,
            "show_value": True,
            "y_axis_format": ".0%",
            "logAxis": False,
            "minorSplitLine": False,
            "truncateYAxis": False,
            "markerEnabled": True,
            "markerSize": 5,
            "color_scheme": "supersetColors",
        }
        query = {
            "filters": [],
            "extras": {"having": "", "where": ""},
            "applied_time_extras": {},
            "columns": [
                {
                    "timeGrain": "P1D",
                    "columnType": "BASE_AXIS",
                    "sqlExpression": "cohort_date",
                    "label": "cohort_date",
                    "expressionType": "SQL",
                }
            ],
            "metrics": metrics,
            "orderby": [[metrics[0], False]],
            "annotation_layers": [],
            "row_limit": 10000,
            "series_columns": [],
            "series_limit": 0,
            "order_desc": False,
            "url_params": {},
            "custom_params": {},
            "custom_form_data": {},
            "time_offsets": [],
        }
        chart.viz_type = "echarts_timeseries_line"
        chart.params = json.dumps(params, ensure_ascii=False)
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
        chart.description = "按 cohort 日期展示次日、7 日、15 日和 30 日广告主留存率"

        root = "ROOT_ID"
        grid = "GRID_ID"
        row = "ROW_RETENTION"
        node = f"CHART-{chart.id}"
        layout = {
            root: {"id": root, "type": "ROOT", "children": [grid]},
            grid: {"id": grid, "type": "GRID", "parents": [root], "children": [row]},
            row: {
                "id": row,
                "type": "ROW",
                "parents": [root, grid],
                "children": [node],
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            },
            node: {
                "id": node,
                "type": "CHART",
                "parents": [root, grid, row],
                "children": [],
                "meta": {
                    "chartId": chart.id,
                    "sliceName": chart.slice_name,
                    "uuid": str(chart.uuid),
                    "width": 12,
                    "height": 64,
                },
            },
            "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": DASHBOARD_TITLE}},
            "DASHBOARD_VERSION_KEY": "v2",
        }
        dashboard.position_json = json.dumps(layout, ensure_ascii=False)
        dashboard.json_metadata = json.dumps(
            {
                "chart_configuration": {
                    str(chart.id): {
                        "id": chart.id,
                        "crossFilters": {"scope": "global", "chartsInScope": []},
                    }
                },
                "global_chart_configuration": {
                    "scope": {"rootPath": [root], "excluded": []},
                    "chartsInScope": [chart.id],
                },
                "color_scheme": "supersetColors",
                "shared_label_colors": {},
                "color_scheme_domain": [],
                "expanded_slices": {},
                "label_colors": {},
                "timed_refresh_immune_slices": [],
                "default_filters": "{}",
                "refresh_frequency": 300,
                "cross_filters_enabled": True,
                "native_filter_configuration": [],
            },
            ensure_ascii=False,
        )
        dashboard.published = True
        dashboard.description = "广告主 cohort 留存率趋势分析"
        dashboard.created_by_fk = owner.id
        dashboard.changed_by_fk = owner.id
        dashboard.owners = [owner]
        dashboard.slices = [chart]
        chart.created_by_fk = owner.id
        chart.changed_by_fk = owner.id
        chart.owners = [owner]
        db.session.commit()
        print(
            f"Bootstrapped dashboard '{DASHBOARD_TITLE}' "
            f"(id={dashboard.id}) with chart id={chart.id}."
        )


if __name__ == "__main__":
    main()
