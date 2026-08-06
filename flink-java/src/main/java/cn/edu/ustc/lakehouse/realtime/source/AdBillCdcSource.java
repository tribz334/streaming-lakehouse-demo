package cn.edu.ustc.lakehouse.realtime.source;

import cn.edu.ustc.lakehouse.realtime.config.RealtimeJobConfig;
import cn.edu.ustc.lakehouse.realtime.model.AdBill;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.time.LocalDateTime;
import java.time.ZoneId;

/** Reads authoritative advertising cost from MySQL instead of SDK ad_log. */
public final class AdBillCdcSource {
    private AdBillCdcSource() {}

    public static DataStream<AdBill> create(
            StreamTableEnvironment tableEnvironment, RealtimeJobConfig config) {
        tableEnvironment.executeSql(createTableDdl(config));
        Table bill = tableEnvironment.from("mysql_ad_bill_cdc");
        return tableEnvironment.toChangelogStream(bill)
                .filter(row -> row.getKind() == RowKind.INSERT)
                .name("mysql ad bill CDC changelog")
                .map(row -> toAdBill(row, config.getOrderTimeZone()))
                .returns(AdBill.class)
                .name("extract authoritative ad_bill");
    }

    private static AdBill toAdBill(Row row, String timeZone) {
        String billId = string(row, 0);
        LocalDateTime billTime = (LocalDateTime) row.getField(9);
        AdBill detail = new AdBill();
        detail.setBillId(billId);
        detail.setBillTimeMillis(
                billTime.atZone(ZoneId.of(timeZone)).toInstant().toEpochMilli());
        detail.setAdvertiserId(string(row, 1));
        detail.setCampaignId(string(row, 2));
        detail.setUnitId(string(row, 3));
        detail.setCreativeId(string(row, 4));
        detail.setUserId(string(row, 5));
        detail.setMedia(string(row, 6));
        detail.setCommerceScene(string(row, 7));
        detail.setCost((java.math.BigDecimal) row.getField(8));
        return detail;
    }

    private static String string(Row row, int position) {
        Object value = row.getField(position);
        return value == null ? null : value.toString();
    }

    private static String createTableDdl(RealtimeJobConfig config) {
        return String.format("""
                CREATE TEMPORARY TABLE mysql_ad_bill_cdc (
                  bill_id STRING,
                  advertiser_id STRING,
                  campaign_id STRING,
                  unit_id STRING,
                  creative_id STRING,
                  user_id STRING,
                  media STRING,
                  commerce_scene STRING,
                  cost DECIMAL(18, 4),
                  bill_time TIMESTAMP(3),
                  updated_at TIMESTAMP(3),
                  PRIMARY KEY (bill_id) NOT ENFORCED
                ) WITH (
                  'connector' = 'mysql-cdc',
                  'hostname' = '%s',
                  'port' = '%d',
                  'username' = '%s',
                  'password' = '%s',
                  'database-name' = '%s',
                  'table-name' = 'ad_bill',
                  'server-id' = '5601-5608',
                  'server-time-zone' = 'UTC',
                  'scan.startup.mode' = 'latest-offset'
                )
                """,
                sql(config.getOrderMysqlHostname()), config.getOrderMysqlPort(),
                sql(config.getOrderMysqlUsername()), sql(config.getOrderMysqlPassword()),
                sql(config.getOrderMysqlDatabase()));
    }

    private static String sql(String value) {
        return value.replace("'", "''");
    }
}
