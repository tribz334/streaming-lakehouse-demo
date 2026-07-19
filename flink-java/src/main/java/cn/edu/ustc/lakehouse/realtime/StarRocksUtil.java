package cn.edu.ustc.lakehouse.realtime;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public final class StarRocksUtil {
    private StarRocksUtil() {}

    public static void sink(
            StreamTableEnvironment tableEnvironment,
            DataStream<RealtimeMetric> metricStream,
            RealtimeJobConfig config) {
        tableEnvironment.executeSql(String.format("""
                CREATE TEMPORARY TABLE starrocks_realtime_metric_sink (
                  window_start TIMESTAMP(3),
                  advertiser_id STRING,
                  campaign_id STRING,
                  unit_id STRING,
                  creative_id STRING,
                  window_end TIMESTAMP(3),
                  advertiser_name STRING,
                  spend DECIMAL(18,4),
                  gmv DECIMAL(18,2),
                  impressions BIGINT,
                  clicks BIGINT,
                  conversions BIGINT,
                  orders BIGINT,
                  ctr DECIMAL(18,6),
                  cvr DECIMAL(18,6),
                  roi DECIMAL(18,6),
                  updated_at TIMESTAMP(3)
                ) WITH (
                  'connector' = 'jdbc',
                  'url' = '%s',
                  'table-name' = '%s',
                  'driver' = 'com.mysql.cj.jdbc.Driver',
                  'username' = '%s',
                  'password' = '%s',
                  'sink.buffer-flush.max-rows' = '100',
                  'sink.buffer-flush.interval' = '1000',
                  'sink.max-retries' = '3'
                )
                """,
                sqlOption(config.getStarRocksJdbcUrl()),
                sqlOption(config.getStarRocksTable()),
                sqlOption(config.getStarRocksUsername()),
                sqlOption(config.getStarRocksPassword())));

        Schema metricSchema = Schema.newBuilder()
                .column("windowStart", DataTypes.TIMESTAMP(3))
                .column("windowEnd", DataTypes.TIMESTAMP(3))
                .column("advertiserId", DataTypes.STRING())
                .column("advertiserName", DataTypes.STRING())
                .column("campaignId", DataTypes.STRING())
                .column("unitId", DataTypes.STRING())
                .column("creativeId", DataTypes.STRING())
                .column("spend", DataTypes.DECIMAL(18, 4))
                .column("gmv", DataTypes.DECIMAL(18, 2))
                .column("impressions", DataTypes.BIGINT())
                .column("clicks", DataTypes.BIGINT())
                .column("conversions", DataTypes.BIGINT())
                .column("orders", DataTypes.BIGINT())
                .column("ctr", DataTypes.DECIMAL(18, 6))
                .column("cvr", DataTypes.DECIMAL(18, 6))
                .column("roi", DataTypes.DECIMAL(18, 6))
                .column("updatedAt", DataTypes.TIMESTAMP(3))
                .build();
        Table metricTable = tableEnvironment.fromDataStream(metricStream, metricSchema);
        tableEnvironment.createTemporaryView("realtime_metric_stream", metricTable);

        tableEnvironment.executeSql("""
                INSERT INTO starrocks_realtime_metric_sink
                SELECT
                  windowStart,
                  advertiserId,
                  campaignId,
                  unitId,
                  creativeId,
                  windowEnd,
                  advertiserName,
                  spend,
                  gmv,
                  impressions,
                  clicks,
                  conversions,
                  orders,
                  ctr,
                  cvr,
                  roi,
                  updatedAt
                FROM realtime_metric_stream
                """);
    }

    private static String sqlOption(String value) {
        return value.replace("'", "''");
    }
}
