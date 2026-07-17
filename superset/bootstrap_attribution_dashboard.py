import json
import uuid

from superset.app import create_app

from dashboard_bootstrap_common import (
    add_row,
    bar_chart,
    create_chart,
    dashboard_metadata,
    metric_card,
    pie_chart,
    remove_legacy_dashboard,
    select_filter,
    table_chart,
    time_filter,
    timeseries,
)


DASHBOARD_TITLE = "广告归因分析大盘"
DASHBOARD_SLUG = "ad-attribution-analysis"
LEGACY_DASHBOARD_TITLE = "广告反作弊与归因分析"
DEFAULT_TIME_RANGE = "2026-06-01 : 2026-07-17"
ATTRIBUTION_COLORS = {
    "自然订单": "#4EA8DE",
    "30分钟直接归因": "#52B788",
    "1日间接归因": "#F9C74F",
    "3日间接归因": "#F9844A",
    "7日间接归因": "#F25F5C",
    "30日间接归因": "#90E0EF",
}


def main():
    app = create_app()
    with app.app_context():
        from flask_appbuilder.security.sqla.models import User
        from superset import db
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice

        datasets = {
            table.table_name: table
            for table in db.session.query(SqlaTable)
            .filter(
                SqlaTable.schema == "ad_ads",
                SqlaTable.table_name.in_([
                    "v_attribution_summary",
                    "v_order_attribution_detail",
                ]),
            )
            .all()
        }
        missing = {"v_attribution_summary", "v_order_attribution_detail"} - datasets.keys()
        if missing:
            raise RuntimeError(f"Missing Superset attribution datasets: {sorted(missing)}")

        summary = datasets["v_attribution_summary"]
        detail = datasets["v_order_attribution_detail"]
        owner = db.session.query(User).filter_by(username="admin").one()
        remove_legacy_dashboard(db, Dashboard, LEGACY_DASHBOARD_TITLE)

        dashboard = db.session.query(Dashboard).filter_by(dashboard_title=DASHBOARD_TITLE).one_or_none()
        if dashboard is None:
            dashboard = Dashboard(
                dashboard_title=DASHBOARD_TITLE,
                slug=DASHBOARD_SLUG,
                published=True,
                created_by_fk=owner.id,
                changed_by_fk=owner.id,
                uuid=uuid.uuid4(),
            )
            db.session.add(dashboard)
            db.session.flush()
        else:
            dashboard.slug = DASHBOARD_SLUG

        specs = [
            metric_card(summary, "订单总 GMV", "total_order_gmv", "全部订单成交金额", DEFAULT_TIME_RANGE),
            metric_card(summary, "广告归因 GMV", "attributed_gmv", "30 天 LastClick 归因", DEFAULT_TIME_RANGE),
            metric_card(summary, "归因订单占比", "attributed_order_rate", "直接 + 间接归因", DEFAULT_TIME_RANGE, ".1%"),
            metric_card(summary, "30 分钟直接归因 GMV", "direct_gmv", "点击后 30 分钟内下单", DEFAULT_TIME_RANGE),
            metric_card(summary, "1-30 天间接归因 GMV", "indirect_gmv", "点击 30 分钟后下单", DEFAULT_TIME_RANGE),
            metric_card(summary, "自然订单 GMV", "organic_gmv", "30 天内没有匹配点击", DEFAULT_TIME_RANGE),
            timeseries(
                summary,
                "归因 GMV 日趋势",
                ["attributed_gmv", "direct_gmv", "indirect_gmv", "organic_gmv"],
                "event_date",
                DEFAULT_TIME_RANGE,
            ),
            timeseries(
                summary,
                "归因订单量日趋势",
                ["total_orders", "attributed_orders"],
                "event_date",
                DEFAULT_TIME_RANGE,
            ),
            pie_chart(
                summary, "各归因窗口 GMV 占比", "total_order_gmv",
                "attribution_period", DEFAULT_TIME_RANGE, label_colors=ATTRIBUTION_COLORS,
            ),
            pie_chart(
                summary, "各归因窗口订单量占比", "total_orders",
                "attribution_period", DEFAULT_TIME_RANGE, label_colors=ATTRIBUTION_COLORS,
            ),
            bar_chart(summary, "归因窗口 GMV 对比", "total_order_gmv", ["attribution_period"], DEFAULT_TIME_RANGE),
            bar_chart(summary, "广告主归因 GMV 排名", "attributed_gmv", ["advertiser_name"], DEFAULT_TIME_RANGE),
            table_chart(
                detail,
                "订单归因下钻明细",
                [
                    "event_date",
                    "attribution_period",
                    "order_advertiser_name",
                    "order_campaign_name",
                    "campaign_name",
                    "creative_id",
                    "order_id",
                    "user_id",
                    "order_ts",
                    "click_ts",
                    "lag_minutes",
                ],
                ["order_count", "order_gmv", "avg_lag_minutes"],
                DEFAULT_TIME_RANGE,
                row_limit=1000,
            ),
        ]
        charts = [
            create_chart(db, Slice, spec, "30 天 LastClick 订单归因专题图表")
            for spec in specs
        ]

        root = "ROOT_ID"
        grid = "GRID_ID"
        layout = {
            root: {"id": root, "type": "ROOT", "children": [grid]},
            grid: {"id": grid, "type": "GRID", "parents": [root], "children": []},
            "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": DASHBOARD_TITLE}},
            "DASHBOARD_VERSION_KEY": "v2",
        }
        rows = [
            ("ROW_ATTRIBUTION_KPI", charts[0:3], 24),
            ("ROW_ATTRIBUTION_TYPE_KPI", charts[3:6], 24),
            ("ROW_ATTRIBUTION_TREND", charts[6:8], 52),
            ("ROW_ATTRIBUTION_SHARE", charts[8:10], 52),
            ("ROW_ATTRIBUTION_RANK", charts[10:12], 46),
            ("ROW_ATTRIBUTION_DETAIL", charts[12:13], 72),
        ]
        for row_id, row_charts, height in rows:
            add_row(layout, root, grid, row_id, row_charts, height)
            layout[grid]["children"].append(row_id)

        native_filters = [
            time_filter(
                "NATIVE_FILTER-ATTRIBUTION-TIME",
                "下单日期",
                [summary.id, detail.id],
                DEFAULT_TIME_RANGE,
                root,
            ),
            select_filter(
                "NATIVE_FILTER-ATTRIBUTION-WINDOW",
                "归因窗口",
                [(summary.id, "attribution_period"), (detail.id, "attribution_period")],
                root,
            ),
            select_filter(
                "NATIVE_FILTER-ATTRIBUTION-ADVERTISER",
                "订单广告主",
                [(summary.id, "advertiser_name"), (detail.id, "order_advertiser_name")],
                root,
            ),
            select_filter(
                "NATIVE_FILTER-ATTRIBUTION-CAMPAIGN",
                "订单计划",
                [(summary.id, "campaign_name"), (detail.id, "order_campaign_name")],
                root,
            ),
        ]

        dashboard.position_json = json.dumps(layout, ensure_ascii=False)
        metadata = dashboard_metadata(charts, root, native_filters)
        metadata["label_colors"] = ATTRIBUTION_COLORS
        metadata["shared_label_colors"] = ATTRIBUTION_COLORS
        metadata["color_scheme_domain"] = list(ATTRIBUTION_COLORS.values())
        dashboard.json_metadata = json.dumps(metadata, ensure_ascii=False)
        dashboard.published = True
        dashboard.description = (
            "独立广告归因分析大盘：展示 30 分钟直接归因、1/3/7/30 日间接归因、"
            "自然订单占比与趋势，并支持下钻到订单和最后广告点击。"
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
