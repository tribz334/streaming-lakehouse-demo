package cn.edu.ustc.lakehouse.realtime.source;

import cn.edu.ustc.lakehouse.realtime.config.RealtimeJobConfig;
import cn.edu.ustc.lakehouse.realtime.model.OrderDetail;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;

/** Builds the authoritative paid-order stream directly from MySQL binlog. */
public final class OrderCdcSource {
    private OrderCdcSource() {}

    public static DataStream<OrderDetail> createOrderDetails(
            StreamTableEnvironment tableEnvironment, RealtimeJobConfig config) {
        tableEnvironment.executeSql(createTableDdl(config));
        Table orders = tableEnvironment.from("mysql_order_cdc");

        return tableEnvironment.toChangelogStream(orders)
                .filter(row -> row.getKind() == RowKind.INSERT || row.getKind() == RowKind.UPDATE_AFTER)
                .name("mysql order CDC changelog")
                .map(row -> toPaidOrder(row, config.getOrderTimeZone()))
                .returns(OrderDetail.class)
                .filter(event -> event != null)
                .name("extract paid commerce orders from MySQL order CDC")
                .keyBy(OrderDetail::getOrderId)
                .process(new PaidOrderDeduplicateFunction())
                .name("deduplicate paid order lifecycle updates");
    }

    private static OrderDetail toPaidOrder(Row row, String timeZone) {
        Integer status = (Integer) row.getField(12);
        LocalDateTime createTime = (LocalDateTime) row.getField(13);
        LocalDateTime paymentTime = (LocalDateTime) row.getField(15);
        if (createTime == null || paymentTime == null || status == null || status < 2) {
            return null;
        }

        String orderId = string(row, 0);
        ZoneId zoneId = ZoneId.of(timeZone);
        OrderDetail order = new OrderDetail();
        order.setEventId(orderId);
        order.setOrderId(orderId);
        order.setUserId(string(row, 1));
        order.setProductId(string(row, 2));
        order.setOrderGmv(BigDecimal.valueOf((Long) row.getField(6)));
        order.setCreateTimeMillis(createTime.atZone(zoneId).toInstant().toEpochMilli());
        order.setPaymentTimeMillis(paymentTime.atZone(zoneId).toInstant().toEpochMilli());
        order.setAttributionStatus("pending");
        return order;
    }

    private static String string(Row row, int position) {
        Object value = row.getField(position);
        return value == null ? null : value.toString();
    }

    private static String createTableDdl(RealtimeJobConfig config) {
        return String.format("""
                CREATE TEMPORARY TABLE mysql_order_cdc (
                  order_id BIGINT,
                  user_id BIGINT,
                  product_id BIGINT,
                  shop_id BIGINT,
                  product_price BIGINT,
                  product_num INT,
                  total_amount BIGINT,
                  payment_method INT,
                  receiver_name STRING,
                  receiver_phone STRING,
                  shipping_address STRING,
                  tracking_number STRING,
                  order_status INT,
                  create_time TIMESTAMP(3),
                  cancel_time TIMESTAMP(3),
                  payment_time TIMESTAMP(3),
                  confirm_time TIMESTAMP(3),
                  refund_time TIMESTAMP(3),
                  refund_finish_time TIMESTAMP(3),
                  finish_time TIMESTAMP(3),
                  updated_at TIMESTAMP(3),
                  PRIMARY KEY (order_id) NOT ENFORCED
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
                  'scan.startup.mode' = '%s'
                )
                """,
                sql(config.getOrderMysqlHostname()), config.getOrderMysqlPort(),
                sql(config.getOrderMysqlUsername()), sql(config.getOrderMysqlPassword()),
                sql(config.getOrderMysqlDatabase()), sql(config.getOrderMysqlTable()),
                sql(config.getOrderMysqlServerId()), sql(config.getOrderMysqlStartupMode()));
    }

    private static String sql(String value) {
        return value.replace("'", "''");
    }

    private static final class PaidOrderDeduplicateFunction
            extends KeyedProcessFunction<String, OrderDetail, OrderDetail> {
        private transient ValueState<String> emittedPaymentEvent;

        @Override
        public void open(OpenContext openContext) {
            ValueStateDescriptor<String> descriptor =
                    new ValueStateDescriptor<>("emitted-payment-event", String.class);
            descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofDays(31)).build());
            emittedPaymentEvent = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(
                OrderDetail event, Context context, Collector<OrderDetail> output)
                throws Exception {
            if (!event.getEventId().equals(emittedPaymentEvent.value())) {
                emittedPaymentEvent.update(event.getEventId());
                output.collect(event);
            }
        }
    }
}
