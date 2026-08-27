package cn.edu.ustc.lakehouse.realtime.job;

import cn.edu.ustc.lakehouse.realtime.config.RealtimeJobConfig;
import cn.edu.ustc.lakehouse.realtime.dwd.LastClickAttributionFunction;
import cn.edu.ustc.lakehouse.realtime.model.AdClickEvent;
import cn.edu.ustc.lakehouse.realtime.model.AttributedOrder;
import cn.edu.ustc.lakehouse.realtime.model.OrderDetail;
import cn.edu.ustc.lakehouse.realtime.util.FlussTableUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

/** Fluss DWD click + PAY streams -> six-hour DataStream LastClick -> Fluss PK table. */
public final class DwdOrderAttributionJob {
    private DwdOrderAttributionJob() {}

    public static void main(String[] args) {
        RealtimeJobConfig config = RealtimeJobConfig.fromArgs(args);
        FlussTableUtil.Context context = FlussTableUtil.createContext(config);
        context.tableEnv().getConfig().set("pipeline.name", "fluss-last-click-datastream-6h");
        String database = config.flussDatabase();

        Table clickTable = context.tableEnv().sqlQuery(
                "SELECT event_id,uid,product_id,creative_id,slot_id,unit_id,campaign_id,"
                        + "advertiser_id,placement_type,ad_type,ts FROM fluss." + database
                        + ".dwd_ad_event_di" + FlussTableUtil.scanHint(config)
                        + " WHERE event_type='click' AND uid IS NOT NULL AND product_id IS NOT NULL");
        DataStream<AdClickEvent> clicks = context.tableEnv().toChangelogStream(clickTable)
                .filter(DwdOrderAttributionJob::isPositiveChange)
                .map(DwdOrderAttributionJob::toClick)
                .returns(AdClickEvent.class)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<AdClickEvent>forBoundedOutOfOrderness(config.outOfOrderness())
                                .withIdleness(config.sourceIdleness())
                                .withTimestampAssigner((event, previous) -> event.clickTimeMillis))
                .name("fluss-dwd-click-source");

        Table orderTable = context.tableEnv().sqlQuery(
                "SELECT order_id,uid,product_id,shop_id,product_price,product_num,total_amount,"
                        + "payment_method,receiver_name,receiver_phone,shipping_address,tracking_number,"
                        + "order_status,create_time,cancel_time,pay_time,confirm_time,refund_time,updated_at,dt,"
                        + "UNIX_TIMESTAMP(pay_time)*1000 AS pay_time_millis,"
                        + "UNIX_TIMESTAMP(pay_time)*1000 AS event_time_millis FROM fluss." + database
                        + ".dwd_ad_order_di" + FlussTableUtil.scanHint(config)
                        + " WHERE order_type='PAY' AND pay_time IS NOT NULL");
        DataStream<OrderDetail> orders = context.tableEnv().toChangelogStream(orderTable)
                .filter(DwdOrderAttributionJob::isPositiveChange)
                .map(DwdOrderAttributionJob::toOrder)
                .returns(OrderDetail.class)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(config.outOfOrderness())
                                .withIdleness(config.sourceIdleness())
                                .withTimestampAssigner((order, previous) -> order.payTimeMillis))
                .name("fluss-dwd-paid-order-source");

        SingleOutputStreamOperator<AttributedOrder> attributed = clicks
                .keyBy(AdClickEvent::key)
                .connect(orders.keyBy(OrderDetail::key))
                .process(new LastClickAttributionFunction(
                        config.attributionAllowedLateness().toMillis()))
                .name("uid-product-last-click-6h");

        context.tableEnv().createTemporaryView("attributed_order", attributed);
        context.tableEnv().executeSql("INSERT INTO fluss." + database + ".dwd_ad_order_acc "
                + "SELECT orderId,uid,productId,shopId,creativeId,slotId,productPrice,productNum,"
                + "totalAmount,paymentMethod,receiverName,receiverPhone,shippingAddress,trackingNumber,"
                + "orderStatus,createTime,cancelTime,payTime,confirmTime,refundTime,updatedAt,"
                + "advertiserId,campaignId,unitId,placementType,adType,clickTime,directAttribution,"
                + "eventTime,dt,`hour` FROM attributed_order");
    }

    private static boolean isPositiveChange(Row row) {
        return row.getKind() == RowKind.INSERT || row.getKind() == RowKind.UPDATE_AFTER;
    }

    private static AdClickEvent toClick(Row row) {
        AdClickEvent click = new AdClickEvent();
        click.eventId = requiredNumber(row, 0).longValue();
        click.uid = requiredNumber(row, 1).longValue();
        click.productId = requiredNumber(row, 2).longValue();
        click.creativeId = requiredNumber(row, 3).longValue();
        click.slotId = requiredNumber(row, 4).longValue();
        click.unitId = requiredNumber(row, 5).longValue();
        click.campaignId = requiredNumber(row, 6).longValue();
        click.advertiserId = requiredNumber(row, 7).longValue();
        click.placementType = requiredNumber(row, 8).intValue();
        click.adType = requiredNumber(row, 9).intValue();
        click.clickTimeMillis = requiredNumber(row, 10).longValue();
        return click;
    }

    private static OrderDetail toOrder(Row row) {
        OrderDetail order = new OrderDetail();
        order.orderId = requiredNumber(row, 0).longValue();
        order.uid = requiredNumber(row, 1).longValue();
        order.productId = requiredNumber(row, 2).longValue();
        order.shopId = requiredNumber(row, 3).longValue();
        order.productPrice = requiredNumber(row, 4).longValue();
        order.productNum = requiredNumber(row, 5).intValue();
        order.totalAmount = requiredNumber(row, 6).longValue();
        order.paymentMethod = requiredNumber(row, 7).intValue();
        order.receiverName = string(row, 8);
        order.receiverPhone = string(row, 9);
        order.shippingAddress = string(row, 10);
        order.trackingNumber = string(row, 11);
        order.orderStatus = requiredNumber(row, 12).intValue();
        order.createTime = string(row, 13);
        order.cancelTime = string(row, 14);
        order.payTime = string(row, 15);
        order.confirmTime = string(row, 16);
        order.refundTime = string(row, 17);
        order.updatedAt = string(row, 18);
        order.dt = string(row, 19);
        order.payTimeMillis = requiredNumber(row, 20).longValue();
        order.eventTimeMillis = requiredNumber(row, 21).longValue();
        return order;
    }

    private static Number requiredNumber(Row row, int position) {
        Object value = row.getField(position);
        if (!(value instanceof Number number)) {
            throw new IllegalArgumentException("Expected numeric field at position " + position + ": " + value);
        }
        return number;
    }

    private static String string(Row row, int position) {
        Object value = row.getField(position);
        return value == null ? null : value.toString();
    }
}
