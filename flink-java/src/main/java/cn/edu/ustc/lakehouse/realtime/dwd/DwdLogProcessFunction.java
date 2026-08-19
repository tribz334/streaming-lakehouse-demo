package cn.edu.ustc.lakehouse.realtime.dwd;

import cn.edu.ustc.lakehouse.realtime.model.AdEvent;
import cn.edu.ustc.lakehouse.realtime.model.PageLogEvent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;

/**
 * Parses the raw SDK report retained in ods_log. Page data is routed to its
 * side output and every item in actions is flattened into one DWD AdEvent.
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
            JsonNode report = objectMapper.readTree(value);
            JsonNode common = requiredObject(report, "common");
            requiredLong(report, "app_id");
            requiredLong(report, "log_id");
            requiredText(report, "report_id");
            requiredLong(report, "ts");

            JsonNode page = report.get("page");
            if (page != null && !page.isNull()) {
                context.output(pageOutputTag, toPageLog(page, common));
            }

            JsonNode actions = report.get("actions");
            if (actions == null || !actions.isArray()) {
                throw new IllegalArgumentException("missing or invalid field: actions");
            }
            for (JsonNode action : actions) {
                output.collect(toAdEvent(action, common));
            }
        } catch (Exception parseError) {
            context.output(dirtyOutputTag, dirtyRecord(value, parseError));
        }
    }

    private AdEvent toAdEvent(JsonNode action, JsonNode common) {
        String actionName = requiredText(action, "action");
        AdEvent event = new AdEvent();
        event.setEventId(requiredText(action, "event_id"));
        event.setEventTimeMillis(requiredLong(action, "ts"));
        event.setUserId(requiredText(common, "uid"));
        event.setSlotId(requiredText(action, "slot_id"));
        event.setCreativeId(requiredText(action, "creative_id"));
        event.setProductId(textOrDefault(action, "product_id", event.getCreativeId()));
        event.setMedia(textOrDefault(action, "media", "unknown"));
        event.setCommerceScene(textOrDefault(action, "commerce_scene", "shop"));
        event.setEventType(normalizeAction(actionName));
        event.setSpend(BigDecimal.ZERO);
        event.setOrderGmv(BigDecimal.ZERO);
        event.setAttributedGmv(BigDecimal.ZERO);
        event.setOrganicGmv(BigDecimal.ZERO);
        return event;
    }

    private PageLogEvent toPageLog(JsonNode page, JsonNode common) {
        PageLogEvent event = new PageLogEvent();
        event.setEventId(requiredText(page, "event_id"));
        event.setEventTimeMillis(requiredLong(page, "ts"));
        event.setUserId(requiredText(common, "uid"));
        event.setEventType(requiredText(page, "event_type"));
        event.setPageId(requiredText(page, "page_id"));
        event.setLastPageId(textOrDefault(page, "last_page_id", null));
        event.setDurationMillis(longOrDefault(page, "during_time", 0L));
        event.setDeviceId(textOrDefault(common, "device_id", "unknown"));
        event.setSource(textOrDefault(page, "source_type", "direct"));
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

    private static String normalizeAction(String action) {
        switch (action) {
            case "send": return "send";
            case "show": return "impression";
            case "click": return "click";
            case "convert": return "conversion";
            default: throw new IllegalArgumentException("unsupported action: " + action);
        }
    }

    private static long longOrDefault(JsonNode node, String field, long defaultValue) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? defaultValue : value.asLong();
    }

    private static long requiredLong(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull() || !value.canConvertToLong()) {
            throw new IllegalArgumentException("missing or invalid field: " + field);
        }
        return value.asLong();
    }

    private static JsonNode requiredObject(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.isObject()) {
            throw new IllegalArgumentException("missing or invalid field: " + field);
        }
        return value;
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
