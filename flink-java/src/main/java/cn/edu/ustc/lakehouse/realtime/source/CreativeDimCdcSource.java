package cn.edu.ustc.lakehouse.realtime.source;

import cn.edu.ustc.lakehouse.realtime.config.RealtimeJobConfig;
import cn.edu.ustc.lakehouse.realtime.model.CreativeDimChange;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

/** Builds a continuously updated creative hierarchy from MySQL DIM CDC. */
public final class CreativeDimCdcSource {
    private CreativeDimCdcSource() {}

    public static DataStream<CreativeDimChange> create(
            StreamTableEnvironment tableEnvironment, RealtimeJobConfig config) {
        tableEnvironment.executeSql(creativeTableDdl(config));
        tableEnvironment.executeSql(unitTableDdl(config));
        tableEnvironment.executeSql(campaignTableDdl(config));

        Table hierarchy = tableEnvironment.sqlQuery("""
                SELECT
                  cr.creative_id,
                  u.campaign_id,
                  cr.unit_id,
                  c.advertiser_id
                FROM mysql_dim_creative AS cr
                JOIN mysql_dim_unit AS u
                  ON cr.unit_id = u.unit_id
                JOIN mysql_dim_campaign AS c
                  ON u.campaign_id = c.campaign_id
                """);

        return tableEnvironment.toChangelogStream(hierarchy)
                .map(CreativeDimCdcSource::toChange)
                .returns(CreativeDimChange.class)
                .name("DIM creative hierarchy CDC changelog");
    }

    private static CreativeDimChange toChange(Row row) {
        CreativeDimChange change = new CreativeDimChange();
        change.setCreativeId(string(row, 0));
        change.setCampaignId(string(row, 1));
        change.setUnitId(string(row, 2));
        change.setAdvertiserId(string(row, 3));
        change.setDelete(row.getKind() == RowKind.DELETE || row.getKind() == RowKind.UPDATE_BEFORE);
        return change;
    }

    private static String creativeTableDdl(RealtimeJobConfig config) {
        return mysqlCdcDdl(
                "mysql_dim_creative",
                """
                  creative_id BIGINT,
                  unit_id BIGINT,
                  updated_at TIMESTAMP(3),
                  PRIMARY KEY (creative_id) NOT ENFORCED
                """,
                "creative_info",
                "5701-5708",
                config);
    }

    private static String unitTableDdl(RealtimeJobConfig config) {
        return mysqlCdcDdl(
                "mysql_dim_unit",
                """
                  unit_id BIGINT,
                  campaign_id BIGINT,
                  updated_at TIMESTAMP(3),
                  PRIMARY KEY (unit_id) NOT ENFORCED
                """,
                "unit_info",
                "5721-5728",
                config);
    }

    private static String campaignTableDdl(RealtimeJobConfig config) {
        return mysqlCdcDdl(
                "mysql_dim_campaign",
                """
                  campaign_id BIGINT,
                  advertiser_id BIGINT,
                  updated_at TIMESTAMP(3),
                  PRIMARY KEY (campaign_id) NOT ENFORCED
                """,
                "campaign_info",
                "5711-5718",
                config);
    }

    private static String mysqlCdcDdl(
            String temporaryTable,
            String columns,
            String sourceTable,
            String serverId,
            RealtimeJobConfig config) {
        return String.format("""
                CREATE TEMPORARY TABLE %s (
                %s
                ) WITH (
                  'connector' = 'mysql-cdc',
                  'hostname' = '%s',
                  'port' = '%d',
                  'username' = '%s',
                  'password' = '%s',
                  'database-name' = '%s',
                  'table-name' = '%s',
                  'server-id' = '%s',
                  'server-time-zone' = 'UTC',
                  'scan.startup.mode' = 'initial'
                )
                """,
                temporaryTable,
                columns,
                sql(config.getOrderMysqlHostname()),
                config.getOrderMysqlPort(),
                sql(config.getOrderMysqlUsername()),
                sql(config.getOrderMysqlPassword()),
                sql(config.getOrderMysqlDatabase()),
                sourceTable,
                serverId);
    }

    private static String string(Row row, int position) {
        Object value = row.getField(position);
        return value == null ? null : value.toString();
    }

    private static String sql(String value) {
        return value.replace("'", "''");
    }
}
