import sqlite3

conn = sqlite3.connect("/app/superset_home/superset.db")
cur = conn.cursor()
cur.execute(
    """
    select t.table_name, t.schema, count(c.id) as columns_count
    from tables t
    left join table_columns c on c.table_id = t.id
    where t.table_name in (
        'v_realtime_ad_metrics',
        'v_advertiser_retention',
        'v_attribution_summary',
        'v_fraud_signal_summary',
        'v_data_quality_result',
        'v_data_quality_summary'
    )
    group by t.table_name, t.schema
    order by t.table_name
    """
)
for row in cur.fetchall():
    print(row)
