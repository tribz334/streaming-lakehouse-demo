import json
import uuid
from superset.app import create_app

REALTIME_TITLE = "广告实时核心指标大盘"
OFFLINE_TITLE = "广告离线核心指标大盘"
LEGACY_REALTIME_TITLE = "Real-Time Advertising Performance Dashboard"


def metric_card(datasource, name, metric, subheader, fmt="SMART_NUMBER"):
    return {"slice_name": name, "viz_type": "big_number_total", "params": {
        "datasource": f"{datasource.id}__table", "viz_type": "big_number_total",
        "metric": metric, "subheader": subheader, "y_axis_format": fmt,
        "time_range": "No filter", "adhoc_filters": [], "rolling_type": "None",
        "show_metric_name": False, "header_font_size": 0.28, "subheader_font_size": 0.15,
    }}


def create_chart(db, Slice, datasource, spec):
    chart = db.session.query(Slice).filter_by(slice_name=spec["slice_name"]).order_by(Slice.id).first()
    if chart is None:
        chart = Slice(slice_name=spec["slice_name"], datasource_type="table", datasource_id=datasource.id,
            datasource_name=f"{datasource.schema}.{datasource.table_name}", viz_type=spec["viz_type"],
            created_by_fk=1, changed_by_fk=1, uuid=uuid.uuid4())
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
    metrics = params.get("metrics") or [params["metric"]]
    x_axis = params.get("x_axis")
    chart.query_context = json.dumps({"datasource": {"id": datasource.id, "type": "table"}, "force": False,
        "queries": [{"filters": [], "granularity": x_axis or datasource.main_dttm_col, "extras": {"having": "", "where": ""},
            "applied_time_extras": {}, "columns": [x_axis] if x_axis else [], "metrics": metrics,
            "orderby": [[metrics[0], False]], "annotation_layers": [], "row_limit": 10000,
            "series_columns": [], "series_limit": 0, "order_desc": True, "url_params": {},
            "custom_params": {}, "custom_form_data": {}, "time_offsets": [],
            "time_range": params.get("time_range", "No filter")}], "form_data": params,
        "result_format": "json", "result_type": "full"}, ensure_ascii=False)
    return chart


def layout_for(title, charts):
    layout = {"ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
        "GRID_ID": {"id": "GRID_ID", "type": "GRID", "parents": ["ROOT_ID"], "children": []},
        "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": title}},
        "DASHBOARD_VERSION_KEY": "v2"}
    for row_no, start in enumerate(range(0, len(charts), 4), 1):
        selected = charts[start:start + 4]
        row_id = f"ROW_KPI_{row_no}"
        layout["GRID_ID"]["children"].append(row_id)
        layout[row_id] = {"id": row_id, "type": "ROW", "parents": ["ROOT_ID", "GRID_ID"], "children": [],
            "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        width = 12 // len(selected)
        for chart in selected:
            node = f"CHART-{chart.id}"
            layout[row_id]["children"].append(node)
            layout[node] = {"id": node, "type": "CHART", "parents": ["ROOT_ID", "GRID_ID", row_id], "children": [],
                "meta": {"chartId": chart.id, "sliceName": chart.slice_name, "uuid": str(chart.uuid),
                    "width": width, "height": 24 if chart.viz_type == "big_number_total" else 56}}
    return layout


def ensure_dashboard(db, Dashboard, Slice, User, datasource, title, slug, specs, aliases=(), dt_filter=False):
    dashboard = db.session.query(Dashboard).filter(Dashboard.dashboard_title.in_([title, *aliases])).order_by(Dashboard.id).first()
    if dashboard is None:
        dashboard = Dashboard(dashboard_title=title, slug=slug, published=True, created_by_fk=1, changed_by_fk=1, uuid=uuid.uuid4())
        db.session.add(dashboard)
        db.session.flush()
    charts = [create_chart(db, Slice, datasource, spec) for spec in specs]
    owner = db.session.query(User).filter_by(username="admin").one()
    dashboard.dashboard_title = title
    dashboard.position_json = json.dumps(layout_for(title, charts), ensure_ascii=False)
    native_filters = []
    if dt_filter:
        native_filters = [{"id": "NATIVE_FILTER-dt", "filterType": "filter_time", "name": "业务日期",
            "targets": [{"datasetId": datasource.id, "column": {"name": "dt"}}], "defaultDataMask": {"extraFormData": {}, "filterState": {}},
            "controlValues": {"enableEmptyFilter": True}, "scope": {"rootPath": ["ROOT_ID"], "excluded": []}, "cascadeParentIds": []}]
    dashboard.json_metadata = json.dumps({"refresh_frequency": 10 if title == REALTIME_TITLE else 0,
        "native_filter_configuration": native_filters, "chart_configuration": {}, "color_scheme": "supersetColors"}, ensure_ascii=False)
    dashboard.published = True
    dashboard.created_by_fk = owner.id
    dashboard.changed_by_fk = owner.id
    dashboard.owners = [owner]
    dashboard.slices = charts
    for chart in charts:
        chart.created_by_fk = owner.id
        chart.changed_by_fk = owner.id
        chart.owners = [owner]


def main():
    app = create_app()
    with app.app_context():
        from superset import db
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice
        from flask_appbuilder.security.sqla.models import User
        realtime = db.session.query(SqlaTable).filter_by(table_name="v_realtime_metric", schema="ad_ads").one()
        offline = db.session.query(SqlaTable).filter_by(table_name="v_offline_metric", schema="ad_ads").one()
        realtime_specs = [
            metric_card(realtime, "总消耗", "total_cost", "实时累计"), metric_card(realtime, "闭环消耗", "total_closed_cost", "实时累计"),
            metric_card(realtime, "广告 GMV", "total_pay_order_gmv", "实时累计"), metric_card(realtime, "实时 ROAS", "realtime_roas", "GMV / 闭环消耗", ",.2f"),
            metric_card(realtime, "短视频广告 GMV", "total_short_video_pay_order_gmv", "ad_type=1"), metric_card(realtime, "直播广告 GMV", "total_live_pay_order_gmv", "ad_type=2"),
            metric_card(realtime, "图文广告 GMV", "total_image_text_pay_order_gmv", "ad_type=3"), metric_card(realtime, "搜索广告 GMV", "total_search_pay_order_gmv", "placement_type=1"),
            metric_card(realtime, "其他广告类型 GMV", "total_other_ad_type_pay_order_gmv", "ad_type=4"),
            metric_card(realtime, "开屏广告 GMV", "total_splash_pay_order_gmv", "placement_type=2"), metric_card(realtime, "信息流广告 GMV", "total_feed_pay_order_gmv", "placement_type=3"),
            metric_card(realtime, "激励广告 GMV", "total_rewarded_pay_order_gmv", "placement_type=4"), metric_card(realtime, "横幅广告 GMV", "total_banner_pay_order_gmv", "placement_type=5"),
            metric_card(realtime, "其他位置广告 GMV", "total_other_placement_pay_order_gmv", "placement_type=6"),
        ]
        offline_specs = [
            metric_card(offline, "离线总消耗", "total_cost", "日级"), metric_card(offline, "离线闭环消耗", "total_closed_cost", "日级"), metric_card(offline, "离线广告 GMV", "total_pay_order_gmv", "日级"),
            metric_card(offline, "CTR", "ctr", "点击 / 曝光", ",.2%"), metric_card(offline, "CVR", "cvr", "转化 / 点击", ",.2%"), metric_card(offline, "ROAS", "roas", "GMV / 闭环消耗", ",.2f"),
            metric_card(offline, "离线短视频广告 GMV", "total_short_video_pay_order_gmv", "ad_type=1"), metric_card(offline, "离线直播广告 GMV", "total_live_pay_order_gmv", "ad_type=2"),
            metric_card(offline, "离线图文广告 GMV", "total_image_text_pay_order_gmv", "ad_type=3"), metric_card(offline, "离线搜索广告 GMV", "total_search_pay_order_gmv", "placement_type=1"),
            metric_card(offline, "离线开屏广告 GMV", "total_splash_pay_order_gmv", "placement_type=2"), metric_card(offline, "离线信息流广告 GMV", "total_feed_pay_order_gmv", "placement_type=3"),
            metric_card(offline, "离线激励广告 GMV", "total_rewarded_pay_order_gmv", "placement_type=4"), metric_card(offline, "离线横幅广告 GMV", "total_banner_pay_order_gmv", "placement_type=5"),
            metric_card(offline, "离线其他位置广告 GMV", "total_other_placement_pay_order_gmv", "placement_type=6"),
        ]
        ensure_dashboard(db, Dashboard, Slice, User, realtime, REALTIME_TITLE, "ad-realtime-core-metrics", realtime_specs, (LEGACY_REALTIME_TITLE,))
        ensure_dashboard(db, Dashboard, Slice, User, offline, OFFLINE_TITLE, "ad-offline-core-metrics", offline_specs, dt_filter=True)
        db.session.commit()
        print("Bootstrapped realtime and offline classified advertising dashboards.")


if __name__ == "__main__":
    main()
