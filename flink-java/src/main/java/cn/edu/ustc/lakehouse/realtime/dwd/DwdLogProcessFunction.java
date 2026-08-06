package cn.edu.ustc.lakehouse.realtime.dwd;

import cn.edu.ustc.lakehouse.realtime.model.AdEvent;
import cn.edu.ustc.lakehouse.realtime.model.PageLogEvent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

/**
 * DWD log parser and dirty-data splitter, named after section 4.3.3 while
 * retaining this project's advertising-only ods_log schema.
 */
public final class DwdLogProcessFunction extends ProcessFunction<String, AdEvent> {
    private final OutputTag<PageLogEvent> pageOutputTag;
    private final OutputTag<String> dirtyOutputTag;
    private transient ObjectMapper objectMapper;

    public DwdLogProcessFunction(
            OutputTag<PageLogEvent> pageOutputTag,
            OutputTag<String> dirtyOutputTag) {
        this.pageOutputTag = pageOutputTag;
        this.dirtyOutputTag = dirtyOutputTag;
    }

    @Override
    public void open(org.apache.flink.api.common.functions.OpenContext openContext) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public void processElement(String value, Context context, Collector<AdEvent> output) {
        try {
            JsonNode node = objectMapper.readTree(value);
            String eventType = requiredText(node, "event_type");
            // log_type describes the event family and is therefore the routing key.
            // event_type describes the concrete action and may evolve independently
            // (for example page_scroll or button_click can be added without changing
            // this process function).
            String logType = requiredText(node, "log_type");
            if ("page".equals(logType)) {
                context.output(pageOutputTag, toPageLog(node, eventType));
                return;
            }
            if (!"ad".equals(logType) || !isAdAction(eventType)) {
                throw new IllegalArgumentException("unsupported log_type/event_type: "
                        + logType + "/" + eventType);
            }
            AdEvent event = new AdEvent();
            event.setEventId(requiredText(node, "event_id"));
            event.setEventTimeMillis(OffsetDateTime.parse(requiredText(node, "ts"))
                    .toInstant().toEpochMilli());
            event.setUserId(requiredText(node, "user_id"));
            event.setPid(requiredText(node, "pid"));
            event.setCreativeId(requiredText(node, "creative_id"));
            event.setProductId(textOrDefault(node, "product_id", event.getCreativeId()));
            event.setMedia(textOrDefault(node, "media", "unknown"));
            event.setCommerceScene(textOrDefault(node, "commerce_scene", "shop"));
            event.setEventType(eventType);
            event.setSpend(BigDecimal.ZERO);
            event.setOrderGmv(BigDecimal.ZERO);
            event.setAttributedGmv(BigDecimal.ZERO);
            event.setOrganicGmv(BigDecimal.ZERO);
            output.collect(event);
        } catch (Exception parseError) {
            context.output(dirtyOutputTag, dirtyRecord(value, parseError));
        }
    }

    private PageLogEvent toPageLog(JsonNode node, String eventType) {
        PageLogEvent event = new PageLogEvent();
        event.setEventId(requiredText(node, "event_id"));
        event.setEventTimeMillis(OffsetDateTime.parse(requiredText(node, "ts"))
                .toInstant().toEpochMilli());
        event.setUserId(requiredText(node, "user_id"));
        event.setEventType(eventType);
        event.setPageId(requiredText(node, "page_id"));
        event.setLastPageId(textOrDefault(node, "last_page_id", null));
        event.setDurationMillis(longOrDefault(node, "duration_ms", 0L));
        event.setDeviceId(textOrDefault(node, "device_id", "unknown"));
        event.setSource(textOrDefault(node, "source", "direct"));
        return event;
    }

    private String dirtyRecord(String rawLog, Exception error) {
        try {
            return objectMapper.createObjectNode()
                    .put("raw_log", rawLog)
                    .put("error_message", error.getMessage())
                    .put("dirty_time", System.currentTimeMillis())
                    .toString();
        } catch (Exception ignored) {
            return rawLog;
        }
    }

    private static boolean isAdAction(String eventType) {
        return "impression".equals(eventType)
                || "click".equals(eventType)
                || "conversion".equals(eventType);
    }

    private static long longOrDefault(JsonNode node, String field, long defaultValue) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? defaultValue : value.asLong();
    }

    private static String requiredText(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull() || value.asText().isBlank()) {
            throw new IllegalArgumentException("missing field: " + field);
        }
        return value.asText();
    }

    private static String textOrDefault(JsonNode node, String field, String defaultValue) {
        /**
         * 从 JSON 节点中获取指定字段的字符串值。
         * 如果字段不存在、字段值为 null 或空字符串，则返回指定的默认值。
         */
        JsonNode value = node.get(field);
        return value == null || value.isNull() || value.asText().isBlank()
                ? defaultValue
                : value.asText();
    }
}
