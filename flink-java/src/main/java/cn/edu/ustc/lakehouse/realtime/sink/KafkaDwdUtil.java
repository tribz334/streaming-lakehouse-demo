package cn.edu.ustc.lakehouse.realtime.sink;

import cn.edu.ustc.lakehouse.realtime.config.RealtimeJobConfig;
import cn.edu.ustc.lakehouse.realtime.model.AdEvent;
import cn.edu.ustc.lakehouse.realtime.model.OrderDetail;
import cn.edu.ustc.lakehouse.realtime.model.PageLogEvent;

    import com.fasterxml.jackson.databind.ObjectMapper;
    import org.apache.flink.api.common.serialization.SerializationSchema;
    import org.apache.flink.api.common.serialization.SimpleStringSchema;
    import org.apache.flink.connector.base.DeliveryGuarantee;
    import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
    import org.apache.flink.connector.kafka.sink.KafkaSink;
    import org.apache.flink.streaming.api.datastream.DataStream;

    import java.util.LinkedHashMap;
    import java.util.Map;

    /** Writes paper-named DWD facts to Kafka while StarRocks remains the serving sink. */
    public final class KafkaDwdUtil {
        private KafkaDwdUtil() {}

    public static void sinkDwdAdActionLog(DataStream<AdEvent> stream, RealtimeJobConfig config) {
            stream.sinkTo(sink(config, config.getDwdAdActionTopic(), new AdActionSchema()))
                    .name("Kafka dwd_ad_action_log sink");
        }

    public static void sinkDwdOrderDetail(DataStream<OrderDetail> stream, RealtimeJobConfig config) {
            stream.sinkTo(sink(config, config.getDwdOrderDetailTopic(), new OrderDetailSchema()))
                    .name("Kafka dwd_order_detail sink");
        }

    public static void sinkDwdPageLog(DataStream<PageLogEvent> stream, RealtimeJobConfig config) {
            stream.sinkTo(sink(config, config.getDwdPageLogTopic(), new PageLogSchema()))
                    .name("Kafka dwd_page_log sink");
        }

    public static void sinkDwdDirtyLog(DataStream<String> stream, RealtimeJobConfig config) {
            stream.sinkTo(sink(config, config.getDwdDirtyLogTopic(), new SimpleStringSchema()))
                    .name("Kafka dwd_dirty_log sink");
        }

        private static <T> KafkaSink<T> sink(
                RealtimeJobConfig config, String topic, SerializationSchema<T> schema) {
            return KafkaSink.<T>builder()
                    .setBootstrapServers(config.getKafkaBootstrapServers())
                    .setRecordSerializer(KafkaRecordSerializationSchema.<T>builder()
                            .setTopic(topic)
                            .setValueSerializationSchema(schema)
                            .build())
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        }

        private abstract static class JsonSchema<T> implements SerializationSchema<T> {
            private transient ObjectMapper mapper;

            @Override
            public byte[] serialize(T value) {
                try {
                    if (mapper == null) mapper = new ObjectMapper();
                    return mapper.writeValueAsBytes(toMap(value));
                } catch (Exception exception) {
                    throw new IllegalArgumentException("Cannot serialize DWD fact", exception);
                }
            }

            protected abstract Map<String, Object> toMap(T value);
        }

        private static final class AdActionSchema extends JsonSchema<AdEvent> {
            @Override
            protected Map<String, Object> toMap(AdEvent event) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("event_id", event.getEventId());
                row.put("event_time", event.getEventTimeMillis());
                row.put("user_id", event.getUserId());
                row.put("product_id", event.getProductId());
                row.put("advertiser_id", event.getAdvertiserId());
                row.put("campaign_id", event.getCampaignId());
                row.put("unit_id", event.getUnitId());
                row.put("pid", event.getPid());
                row.put("creative_id", event.getCreativeId());
                row.put("media", event.getMedia());
                row.put("commerce_scene", event.getCommerceScene());
                row.put("event_type", event.getEventType());
                return row;
            }
        }

        private static final class OrderDetailSchema extends JsonSchema<OrderDetail> {
            @Override
            protected Map<String, Object> toMap(OrderDetail order) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("event_id", order.getEventId());
                row.put("order_id", order.getOrderId());
                row.put("user_id", order.getUserId());
                row.put("product_id", order.getProductId());
                row.put("advertiser_id", order.getAdvertiserId());
                row.put("creative_id", order.getCreativeId());
                row.put("campaign_id", order.getCampaignId());
                row.put("unit_id", order.getUnitId());
                row.put("pid", order.getPid());
                row.put("create_time", order.getCreateTimeMillis());
                row.put("payment_time", order.getPaymentTimeMillis());
                row.put("click_event_id", order.getAttributedClickEventId());
                row.put("click_time", order.getAttributedClickTimeMillis());
                row.put("attribution_status", order.getAttributionStatus());
                row.put("gmv", order.getOrderGmv());
                return row;
            }
        }

        private static final class PageLogSchema extends JsonSchema<PageLogEvent> {
            @Override
            protected Map<String, Object> toMap(PageLogEvent event) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("event_id", event.getEventId());
                row.put("event_time", event.getEventTimeMillis());
                row.put("user_id", event.getUserId());
                row.put("event_type", event.getEventType());
                row.put("page_id", event.getPageId());
                row.put("last_page_id", event.getLastPageId());
                row.put("duration_ms", event.getDurationMillis());
                row.put("device_id", event.getDeviceId());
                row.put("source", event.getSource());
                return row;
            }
        }
    }
