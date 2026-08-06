package cn.edu.ustc.lakehouse.realtime.sink;

import cn.edu.ustc.lakehouse.realtime.config.RealtimeJobConfig;
import cn.edu.ustc.lakehouse.realtime.model.RealtimeMetric;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.sql.Timestamp;
import java.sql.Types;

/** StarRocks serving sink attached to the same DataStream job as both Kafka DWD sinks. */
public final class StarRocksUtil {
    private StarRocksUtil() {}

    public static void sink(DataStream<RealtimeMetric> stream, RealtimeJobConfig config) {
        String sql = """
                INSERT INTO %s (
                  window_start, advertiser_id, campaign_id, unit_id, creative_id,
                  media, commerce_scene, window_end, spend,
                  order_gmv, attributed_gmv, organic_gmv, impressions, clicks,
                  paid_orders, attributed_orders, organic_orders, ctr, cvr, roi, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """.formatted(config.getStarRocksTable());

        JdbcSink<RealtimeMetric> sink = JdbcSink.<RealtimeMetric>builder()
                .withQueryStatement(sql, (statement, metric) -> {
                    statement.setTimestamp(1, Timestamp.valueOf(metric.getWindowStart()));
                    statement.setString(2, metric.getAdvertiserId());
                    statement.setString(3, metric.getCampaignId());
                    statement.setString(4, metric.getUnitId());
                    statement.setString(5, metric.getCreativeId());
                    statement.setString(6, metric.getMedia());
                    statement.setString(7, metric.getCommerceScene());
                    statement.setTimestamp(8, Timestamp.valueOf(metric.getWindowEnd()));
                    statement.setBigDecimal(9, metric.getSpend());
                    statement.setBigDecimal(10, metric.getOrderGmv());
                    statement.setBigDecimal(11, metric.getAttributedGmv());
                    statement.setBigDecimal(12, metric.getOrganicGmv());
                    statement.setLong(13, metric.getImpressions());
                    statement.setLong(14, metric.getClicks());
                    statement.setLong(15, metric.getPaidOrders());
                    statement.setLong(16, metric.getAttributedOrders());
                    statement.setLong(17, metric.getOrganicOrders());
                    setDecimal(statement, 18, metric.getCtr());
                    setDecimal(statement, 19, metric.getCvr());
                    setDecimal(statement, 20, metric.getRoi());
                    statement.setTimestamp(21, Timestamp.valueOf(metric.getUpdatedAt()));
                })
                .withExecutionOptions(JdbcExecutionOptions.builder()
                        // StarRocks creates a new tablet version per committed
                        // load. Larger, less frequent batches prevent the demo
                        // workload from outrunning background compaction.
                        .withBatchSize(1_000)
                        .withBatchIntervalMs(10_000)
                        .withMaxRetries(3)
                        .build())
                .buildAtLeastOnce(new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(config.getStarRocksJdbcUrl())
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername(config.getStarRocksUsername())
                        .withPassword(config.getStarRocksPassword())
                        .build());

        stream.sinkTo(sink).name("StarRocks realtime_ad_attribution_metrics_10s sink");
    }

    private static void setDecimal(
            java.sql.PreparedStatement statement, int index, java.math.BigDecimal value)
            throws java.sql.SQLException {
        if (value == null) statement.setNull(index, Types.DECIMAL);
        else statement.setBigDecimal(index, value);
    }
}
