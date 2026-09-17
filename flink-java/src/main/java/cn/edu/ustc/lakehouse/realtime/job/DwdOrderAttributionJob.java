package cn.edu.ustc.lakehouse.realtime.job;

import cn.edu.ustc.lakehouse.realtime.config.FlussTableNames;
import cn.edu.ustc.lakehouse.realtime.config.RealtimeJobConfig;
import cn.edu.ustc.lakehouse.realtime.dwd.AttributedOrderDimAsyncFunction;
import cn.edu.ustc.lakehouse.realtime.dwd.LastClickAttributionFunction;
import cn.edu.ustc.lakehouse.realtime.dwd.OrderProcessFunction;
import cn.edu.ustc.lakehouse.realtime.model.AdClickEvent;
import cn.edu.ustc.lakehouse.realtime.model.AttributedOrder;
import cn.edu.ustc.lakehouse.realtime.model.DirtyLog;
import cn.edu.ustc.lakehouse.realtime.model.OrderInfo;
import cn.edu.ustc.lakehouse.realtime.util.FlussUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

/** Recognizes paid orders, applies six-hour Last Click attribution and maintains DWD order state. */
public final class DwdOrderAttributionJob {
    private static final ZoneId BUSINESS_ZONE = ZoneId.of("Asia/Shanghai");
    private static final DateTimeFormatter PARTITION_DATE = DateTimeFormatter.BASIC_ISO_DATE;
    private static final TypeInformation<Row> PAID_CHANGE_TYPE = Types.ROW_NAMED(
            new String[]{"event_id", "order_id", "uid", "product_id", "order_type", "pay_time",
                    "pay_order_gmv", "shop_id", "product_price", "product_num", "total_amount",
                    "payment_method", "receiver_name", "receiver_phone", "shipping_address",
                    "tracking_number", "order_status", "create_time", "cancel_time", "confirm_time",
                    "refund_time", "updated_at", "dt"},
            Types.STRING, Types.LONG, Types.LONG, Types.LONG, Types.STRING, Types.STRING,
            Types.LONG, Types.LONG, Types.LONG, Types.INT, Types.LONG, Types.INT,
            Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.INT, Types.STRING,
            Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING);

    private DwdOrderAttributionJob() {}

    public static void main(String[] args) {
        RealtimeJobConfig config = RealtimeJobConfig.fromArgs(args);
        FlussUtil.Context context = FlussUtil.createContext(config);
        context.tableEnv().getConfig().set("pipeline.name", "fluss-order-fact-datastream");
        String database = config.flussDatabase();

        Table orderSourceTable = FlussUtil.lookup(context,
                "SELECT order_id,uid,product_id,shop_id,product_price,product_num,total_amount,"
                        + "payment_method,receiver_name,receiver_phone,shipping_address,tracking_number,"
                        + "order_status,create_time,cancel_time,pay_time,confirm_time,refund_time,updated_at,dt "
                        + "FROM fluss." + database + "." + FlussTableNames.ODS_ORDER_INFO
                        + FlussUtil.scanHint(config));
        DataStream<Row> orderChanges = context.tableEnv().toChangelogStream(orderSourceTable);

        SingleOutputStreamOperator<Row> paidOrderChanges = orderChanges
                .keyBy(DwdOrderAttributionJob::orderId)
                .process(OrderProcessFunction.paidOrders())
                .returns(PAID_CHANGE_TYPE)
                .name("order-paid-change-recognition");

        DataStream<OrderInfo> paidOrders = paidOrderChanges
                .getSideOutput(OrderProcessFunction.PAID_ORDER_OUTPUT)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(config.outOfOrderness())
                                .withIdleness(config.sourceIdleness())
                                .withTimestampAssigner((order, previous) -> order.payTimeMillis));

        Table clickTable = FlussUtil.lookup(context,
                "SELECT e.event_id,e.uid,e.product_id,e.creative_id,e.ts FROM fluss." + database
                        + "." + FlussTableNames.DWD_AD_EVENT + FlussUtil.scanHint(config) + " e "
                        + "WHERE e.event_type='click' AND e.uid IS NOT NULL AND e.product_id IS NOT NULL");
        DataStream<AdClickEvent> clicks = context.tableEnv().toChangelogStream(clickTable)
                .filter(DwdOrderAttributionJob::isPositiveChange)
                .map(DwdOrderAttributionJob::toClick)
                .returns(AdClickEvent.class)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<AdClickEvent>forBoundedOutOfOrderness(config.outOfOrderness())
                                .withIdleness(config.sourceIdleness())
                                .withTimestampAssigner((event, previous) -> event.clickTimeMillis))
                .name("fluss-dwd-click-source");

        SingleOutputStreamOperator<AttributedOrder> attributedOrders = clicks
                .keyBy(AdClickEvent::key)
                .connect(paidOrders.keyBy(OrderInfo::key))
                .process(new LastClickAttributionFunction(
                        config.attributionAllowedLateness().toMillis()))
                .name("uid-product-last-click-6h");

        DataStream<AttributedOrder> enrichedOrders = AsyncDataStream.orderedWait(
                        attributedOrders,
                        new AttributedOrderDimAsyncFunction(config),
                        config.dimLookupTimeout().toMillis(),
                        TimeUnit.MILLISECONDS,
                        config.dimLookupCapacity())
                .name("attributed-order-creative-dim-lookup");

        DataStream<DirtyLog> lateClicks = attributedOrders
                .getSideOutput(LastClickAttributionFunction.LATE_CLICKS)
                .map(DwdOrderAttributionJob::toLateClick)
                .returns(DirtyLog.class)
                .name("late-attribution-click-audit");

        context.tableEnv().createTemporaryView("attributed_order", enrichedOrders);
        context.tableEnv().createTemporaryView("late_attribution_click", lateClicks);
        context.tableEnv().executeSql("CREATE TEMPORARY TABLE late_attribution_click_log ("
                + "rawData STRING,errorReason STRING,errorTime BIGINT,dt STRING) "
                + "WITH ('connector'='print','print-identifier'='late-attribution-click')");

        StatementSet sinks = context.tableEnv().createStatementSet();
        sinks.addInsertSql("INSERT INTO fluss." + database + "." + FlussTableNames.DWD_ORDER + " "
                + "SELECT orderId,uid,productId,shopId,creativeId,unitId,campaignId,advertiserId,"
                + "isClosed,adType,placementType,productPrice,productNum,"
                + "totalAmount,paymentMethod,receiverName,receiverPhone,shippingAddress,trackingNumber,"
                + "orderStatus,createTime,cancelTime,payTime,confirmTime,refundTime,updatedAt,"
                + "dt,`hour` FROM attributed_order");
        sinks.addInsertSql("INSERT INTO late_attribution_click_log "
                + "SELECT rawData,errorReason,errorTime,dt FROM late_attribution_click");
        sinks.execute();
    }

    private static boolean isPositiveChange(Row row) {
        return row.getKind() == RowKind.INSERT || row.getKind() == RowKind.UPDATE_AFTER;
    }

    private static long orderId(Row row) {
        return requiredNumber(row, 0).longValue();
    }

    private static AdClickEvent toClick(Row row) {
        AdClickEvent click = new AdClickEvent();
        click.eventId = requiredNumber(row, 0).longValue();
        click.uid = requiredNumber(row, 1).longValue();
        click.productId = requiredNumber(row, 2).longValue();
        click.creativeId = requiredNumber(row, 3).longValue();
        click.clickTimeMillis = requiredNumber(row, 4).longValue();
        return click;
    }

    private static Number requiredNumber(Row row, int position) {
        Object value = row.getField(position);
        if (value instanceof Number number) return number;
        throw new IllegalArgumentException(
                "Expected numeric field at position " + position + ": " + value);
    }

    private static DirtyLog toLateClick(AdClickEvent click) {
        DirtyLog dirty = new DirtyLog();
        dirty.rawData = "event_id=" + click.eventId + ";creative_id=" + click.creativeId
                + ";uid=" + click.uid + ";product_id=" + click.productId;
        dirty.errorReason = "ATTRIBUTION_CLICK_LATER_THAN_WATERMARK";
        dirty.errorTime = click.clickTimeMillis;
        dirty.dt = PARTITION_DATE.format(Instant.ofEpochMilli(click.clickTimeMillis)
                .atZone(BUSINESS_ZONE).toLocalDate());
        return dirty;
    }
}
