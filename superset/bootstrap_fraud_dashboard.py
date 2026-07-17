import json
import uuid

from superset.app import create_app

from dashboard_bootstrap_common import (
    add_row,
    bar_chart,
    create_chart,
    dashboard_metadata,
    metric_card,
    remove_legacy_dashboard,
    select_filter,
    table_chart,
    time_filter,
    timeseries,
)


DASHBOARD_TITLE = "广告反作弊监控大盘"
DASHBOARD_SLUG = "ad-fraud-monitoring"
LEGACY_DASHBOARD_TITLE = "广告反作弊与归因分析"
DEFAULT_TIME_RANGE = "Last 14 days"


def main():
    app = create_app()
    with app.app_context():
        from flask_appbuilder.security.sqla.models import User
        from superset import db
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice

        fraud = (
            db.session.query(SqlaTable)
            .filter_by(table_name="v_fraud_signal_summary", schema="ad_ads")
            .one()
        )
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
            metric_card(fraud, "可疑用户数", "suspicious_users", "命中反作弊规则的用户", DEFAULT_TIME_RANGE),
            metric_card(fraud, "可疑窗口数", "suspicious_windows", "异常的一分钟流量窗口", DEFAULT_TIME_RANGE),
            metric_card(fraud, "可疑点击总量", "suspicious_clicks", "规则命中的可疑点击", DEFAULT_TIME_RANGE),
            metric_card(fraud, "可疑消耗", "suspicious_spend", "疑似作弊流量产生的消耗", DEFAULT_TIME_RANGE),
            metric_card(fraud, "平均风险评分", "avg_risk_score", "0-1，越高风险越大", DEFAULT_TIME_RANGE, ".3f"),
            timeseries(
                fraud,
                "可疑点击与消耗日趋势",
                ["suspicious_clicks", "suspicious_spend"],
                "event_date",
                DEFAULT_TIME_RANGE,
            ),
            timeseries(
                fraud,
                "可疑用户与窗口日趋势",
                ["suspicious_users", "suspicious_windows"],
                "event_date",
                DEFAULT_TIME_RANGE,
            ),
            bar_chart(fraud, "风险广告主可疑点击排名", "suspicious_clicks", ["advertiser_name"], DEFAULT_TIME_RANGE),
            bar_chart(fraud, "风险广告主可疑消耗排名", "suspicious_spend", ["advertiser_name"], DEFAULT_TIME_RANGE),
            bar_chart(fraud, "广告主平均风险评分", "avg_risk_score", ["advertiser_name"], DEFAULT_TIME_RANGE),
            table_chart(
                fraud,
                "反作弊信号下钻明细",
                ["event_date", "advertiser_id", "advertiser_name"],
                [
                    "suspicious_users",
                    "suspicious_windows",
                    "suspicious_clicks",
                    "suspicious_spend",
                    "avg_risk_score",
                ],
                DEFAULT_TIME_RANGE,
                row_limit=1000,
            ),
        ]
        charts = [
            create_chart(db, Slice, spec, "程序化广告异常点击、异常消耗和风险评分专题图表")
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
            ("ROW_FRAUD_KPI", charts[0:5], 24),
            ("ROW_FRAUD_TREND", charts[5:7], 52),
            ("ROW_FRAUD_RANK", charts[7:10], 48),
            ("ROW_FRAUD_DETAIL", charts[10:11], 72),
        ]
        for row_id, row_charts, height in rows:
            add_row(layout, root, grid, row_id, row_charts, height)
            layout[grid]["children"].append(row_id)

        native_filters = [
            time_filter(
                "NATIVE_FILTER-FRAUD-TIME",
                "事件日期",
                [fraud.id],
                DEFAULT_TIME_RANGE,
                root,
            ),
            select_filter(
                "NATIVE_FILTER-FRAUD-ADVERTISER",
                "风险广告主",
                [(fraud.id, "advertiser_name")],
                root,
            ),
        ]

        dashboard.position_json = json.dumps(layout, ensure_ascii=False)
        dashboard.json_metadata = json.dumps(
            dashboard_metadata(charts, root, native_filters, refresh_frequency=30),
            ensure_ascii=False,
        )
        dashboard.published = True
        dashboard.description = (
            "独立广告反作弊监控大盘：展示异常流量规模、可疑消耗、风险评分、"
            "广告主排名和按日期/广告主下钻的风险信号明细。"
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
