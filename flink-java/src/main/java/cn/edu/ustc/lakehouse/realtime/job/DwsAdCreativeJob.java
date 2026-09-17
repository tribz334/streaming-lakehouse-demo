package cn.edu.ustc.lakehouse.realtime.job;

import cn.edu.ustc.lakehouse.realtime.config.RealtimeJobConfig;
import cn.edu.ustc.lakehouse.realtime.config.FlussTableNames;
import cn.edu.ustc.lakehouse.realtime.dws.AdEventFieldMapper;
import cn.edu.ustc.lakehouse.realtime.dws.BillFieldMapper;
import cn.edu.ustc.lakehouse.realtime.dws.CreativeSummaryAggregator;
import cn.edu.ustc.lakehouse.realtime.dws.OrderFieldMapper;
import cn.edu.ustc.lakehouse.realtime.model.CreativeField;
import cn.edu.ustc.lakehouse.realtime.model.CreativeSummary;
import cn.edu.ustc.lakehouse.realtime.util.FlussUtil;
import cn.edu.ustc.lakehouse.realtime.util.StarRocksUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;

/** Coordinates source mapping, watermarking, window aggregation and DWS publication. */
public final class DwsAdCreativeJob {
    private DwsAdCreativeJob() {}

    public static void main(String[] args) {
        RealtimeJobConfig config = RealtimeJobConfig.fromArgs(args);
        FlussUtil.Context context = FlussUtil.createContext(config);
        context.tableEnv().getConfig().set("pipeline.name", "fluss-realtime-metric-datastream-10s");
        String database = config.flussDatabase();

        Table eventTable = FlussUtil.lookup(context,
                "SELECT e.creative_id,e.event_type,u.placement_type,u.ad_type,e.ts,"
                        + "CONCAT(SUBSTRING(e.dt,1,4),'-',SUBSTRING(e.dt,5,2),'-',"
                        + "SUBSTRING(e.dt,7,2)) FROM fluss." + database
                        + "." + FlussTableNames.DWD_AD_EVENT + FlussUtil.scanHint(config) + " e "
                        + "JOIN fluss." + database + "." + FlussTableNames.DIM_CREATIVE + " c "
                        + "ON e.creative_id=c.creative_id "
                        + "JOIN fluss." + database + "." + FlussTableNames.DIM_UNIT
                        + " u ON c.unit_id=u.unit_id");
        DataStream<CreativeField> eventFields = withWatermarks(
                context.tableEnv().toChangelogStream(eventTable)
                        .filter(AdEventFieldMapper::isMetricEvent)
                        .map(new AdEventFieldMapper()).returns(CreativeField.class), config);

        DataStream<CreativeField> billFields = withWatermarks(
                context.tableEnv().toChangelogStream(BillFieldMapper.readBills(context, config))
                        .map(new BillFieldMapper()).returns(CreativeField.class), config);

        Table orderTable = FlussUtil.lookup(context,
                "SELECT o.row_after.creative_id,'PAY' AS order_type,"
                        + "o.row_after.total_amount AS order_gmv,o.row_after.placement_type,"
                        + "o.row_after.ad_type,"
                        + "UNIX_TIMESTAMP(o.row_after.pay_time)*1000 AS metric_ts,"
                        + "DATE_FORMAT(TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP(o.row_after.pay_time)*1000,3),"
                        + "'yyyy-MM-dd') AS dt "
                        + "FROM (SELECT `before` AS row_before,`after` AS row_after FROM fluss."
                        + database + ".`" + FlussTableNames.DWD_ORDER + "$binlog`"
                        + FlussUtil.scanHint(config) + ") o "
                        + "WHERE o.row_before.pay_time IS NULL AND o.row_after.pay_time IS NOT NULL "
                + "UNION ALL "
                        + "SELECT o.row_after.creative_id,'REFUND',o.row_after.total_amount,"
                        + "o.row_after.placement_type,o.row_after.ad_type,"
                        + "UNIX_TIMESTAMP(o.row_after.refund_time)*1000,"
                        + "DATE_FORMAT(TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP(o.row_after.refund_time)*1000,3),"
                        + "'yyyy-MM-dd') "
                        + "FROM (SELECT `before` AS row_before,`after` AS row_after FROM fluss."
                        + database + ".`" + FlussTableNames.DWD_ORDER + "$binlog`"
                        + FlussUtil.scanHint(config) + ") o "
                        + "WHERE o.row_before.refund_time IS NULL "
                        + "AND o.row_after.refund_time IS NOT NULL");
        DataStream<CreativeField> orderFields = withWatermarks(
                context.tableEnv().toChangelogStream(orderTable)
                        .map(new OrderFieldMapper()).returns(CreativeField.class), config);

        DataStream<CreativeSummary> result =
                buildWindowAggregation(eventFields, billFields, orderFields, config);
        StarRocksUtil.write(context, config, result);
    }

    private static DataStream<CreativeSummary> buildWindowAggregation(
            DataStream<CreativeField> eventFields,
            DataStream<CreativeField> billFields,
            DataStream<CreativeField> orderFields,
            RealtimeJobConfig config) {
        return eventFields.union(billFields, orderFields)
                .keyBy(field -> field.creativeId)
                .window(TumblingEventTimeWindows.of(
                        Time.seconds(config.realtimeMetricWindowSeconds())))
                .aggregate(new CreativeSummaryAggregator.Incremental(),
                        new CreativeSummaryAggregator.WindowResult())
                .name("creative-realtime-metric-window");
    }

    private static DataStream<CreativeField> withWatermarks(
            DataStream<CreativeField> stream, RealtimeJobConfig config) {
        return stream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<CreativeField>forBoundedOutOfOrderness(
                                config.outOfOrderness())
                        .withIdleness(config.sourceIdleness())
                        .withTimestampAssigner((field, previous) -> field.eventTimeMillis));
    }
}
