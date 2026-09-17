package cn.edu.ustc.lakehouse.realtime.util;

import cn.edu.ustc.lakehouse.realtime.config.RealtimeJobConfig;
import cn.edu.ustc.lakehouse.realtime.model.CreativeSummary;
import org.apache.flink.streaming.api.datastream.DataStream;

/** Writes each creative/window exclusively to the StarRocks primary-key table. */
public final class StarRocksUtil {
    private StarRocksUtil() {}

    public static void write(FlussUtil.Context context, RealtimeJobConfig config,
                             DataStream<CreativeSummary> summaries) {
        context.tableEnv().createTemporaryView("creative_summary_window", summaries);
        context.tableEnv().executeSql("""
                CREATE TEMPORARY TABLE starrocks_realtime (
                  dt DATE NOT NULL,
                  window_start TIMESTAMP(3) NOT NULL,
                  creative_id BIGINT NOT NULL,
                  window_end TIMESTAMP(3),
                  delivery_count BIGINT,
                  impression_count BIGINT,
                  click_count BIGINT,
                  conversion_count BIGINT,
                  cost BIGINT,
                  closed_cost BIGINT,
                  pay_order_count BIGINT,
                  refund_order_count BIGINT,
                  pay_order_gmv BIGINT,
                  refund_order_gmv BIGINT,
                  short_video_pay_order_gmv BIGINT,
                  live_pay_order_gmv BIGINT,
                  image_text_pay_order_gmv BIGINT,
                  other_ad_type_pay_order_gmv BIGINT,
                  search_pay_order_gmv BIGINT,
                  splash_pay_order_gmv BIGINT,
                  feed_pay_order_gmv BIGINT,
                  rewarded_pay_order_gmv BIGINT,
                  banner_pay_order_gmv BIGINT,
                  other_placement_pay_order_gmv BIGINT,
                  PRIMARY KEY (dt, window_start, creative_id) NOT ENFORCED
                ) WITH (
                  'connector'='starrocks',
                  'jdbc-url'='jdbc:mysql://starrocks:9030',
                  'load-url'='starrocks:8030',
                  'database-name'='ad_ads',
                  'table-name'='dws_ad_creative_10s',
                  'username'='root',
                  'password'='',
                  'sink.semantic'='at-least-once',
                  'sink.buffer-flush.interval-ms'='1000'
                )
                """);
        // Replayed full window values overwrite the same primary key.
        context.tableEnv().executeSql("""
                INSERT INTO starrocks_realtime
                SELECT CAST(dt AS DATE), CAST(windowStart AS TIMESTAMP(3)), creativeId,
                       CAST(windowEnd AS TIMESTAMP(3)), deliveryCount, impressionCount, clickCount, conversionCount, cost, closedCost, payOrderCount, refundOrderCount, payOrderGmv, refundOrderGmv, shortVideoPayOrderGmv, livePayOrderGmv, imageTextPayOrderGmv, otherAdTypePayOrderGmv, searchPayOrderGmv, splashPayOrderGmv, feedPayOrderGmv, rewardedPayOrderGmv, bannerPayOrderGmv, otherPlacementPayOrderGmv
                FROM creative_summary_window
                """);
    }
}
