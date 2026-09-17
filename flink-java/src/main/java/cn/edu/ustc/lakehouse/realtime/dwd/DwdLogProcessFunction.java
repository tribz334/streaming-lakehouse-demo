package cn.edu.ustc.lakehouse.realtime.dwd;

import cn.edu.ustc.lakehouse.realtime.model.DirtyLog;
import cn.edu.ustc.lakehouse.realtime.model.AdEvent;
import cn.edu.ustc.lakehouse.realtime.model.RawLog;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/** Parses, validates and fans out the JSON payload retained by Fluss ods_log. */
public final class DwdLogProcessFunction extends ProcessFunction<RawLog, AdEvent> {
    private static final ZoneId BUSINESS_ZONE = ZoneId.of("Asia/Shanghai");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.BASIC_ISO_DATE;

    private final OutputTag<DirtyLog> dirtyOutput;
    private transient ObjectMapper mapper;

    public DwdLogProcessFunction(OutputTag<DirtyLog> dirtyOutput) {
        this.dirtyOutput = dirtyOutput;
    }

    @Override
    public void open(OpenContext openContext) {
        mapper = new ObjectMapper();
    }

    @Override
    public void processElement(RawLog raw, Context context, Collector<AdEvent> output) {
        try {
            validateRawLog(raw);
            JsonNode common = parseJson(raw.common, "common");
            JsonNode actions = parseJson(raw.events, "events");
            if (!common.isObject()) {
                throw new IllegalArgumentException("common must be a JSON object");
            }
            if ((!actions.isArray() || actions.isEmpty()) && !actions.isObject()) {
                throw new IllegalArgumentException("events must be a non-empty array or an event object");
            }
            processActions(raw, common, actions, output);
        } catch (Exception error) {
            context.output(dirtyOutput, collectDirty(raw, error));
        }
    }

    private void processActions(
            RawLog raw, JsonNode common, JsonNode actions, Collector<AdEvent> output) {
        if (actions.isObject()) {
            output.collect(parseAction(raw, common, actions, 0));
            return;
        }
        for (int index = 0; index < actions.size(); index++) {
            output.collect(parseAction(raw, common, actions.get(index), index));
        }
    }

    private AdEvent parseAction(RawLog raw, JsonNode common, JsonNode action, int index) {
        if (!action.isObject()) {
            throw new IllegalArgumentException("event at index " + index + " must be a JSON object");
        }
        AdEvent event = new AdEvent();
        event.eventId = index == 0 ? raw.msgId : raw.msgId ^ (0x9E3779B97F4A7C15L * index);
        event.uid = requiredLong(common, "uid");
        event.deviceId = requiredText(common, "device_id");
        event.platform = Math.toIntExact(requiredLong(common, "platform"));
        event.appVc = requiredText(common, "app_version");
        event.browserVc = optionalText(common, "browser_version");
        event.sdkVc = requiredText(common, "sdk_version");
        event.creativeId = requiredLong(action, "creative_id");
        event.productId = requiredLong(action, "product_id");
        event.slotId = requiredLong(action, "slot_id");
        event.eventType = normalizeEvent(requiredText(action, "event"));
        event.ts = requiredLong(action, "ts");
        event.dt = raw.dt;
        return event;
    }

    private JsonNode parseJson(String json, String field) throws Exception {
        if (json == null || json.isBlank()) {
            throw new IllegalArgumentException(field + " is empty");
        }
        return mapper.readTree(json);
    }

    private void validateRawLog(RawLog raw) {
        if (raw == null) {
            throw new IllegalArgumentException("raw log is null");
        }
        if (raw.msgId <= 0 || raw.busId <= 0 || raw.appId <= 0 || raw.logId <= 0 || raw.ts <= 0) {
            throw new IllegalArgumentException("invalid SDK envelope identifier or timestamp");
        }
        if (raw.dt == null || raw.dt.isBlank()) {
            throw new IllegalArgumentException("dt is empty");
        }
    }

    private DirtyLog collectDirty(RawLog raw, Exception error) {
        long errorTime = System.currentTimeMillis();
        DirtyLog dirty = new DirtyLog();
        dirty.rawData = serializeRawLog(raw);
        dirty.errorReason = "PARSE_OR_VALIDATION_ERROR: "
                + (error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage());
        dirty.errorTime = errorTime;
        dirty.dt = DATE_FORMATTER.format(Instant.ofEpochMilli(errorTime).atZone(BUSINESS_ZONE));
        return dirty;
    }

    private String serializeRawLog(RawLog raw) {
        if (raw == null) {
            return "null";
        }
        try {
            return mapper.writeValueAsString(raw);
        } catch (Exception ignored) {
            return "RawLog{msgId=" + raw.msgId + ",busId=" + raw.busId + ",appId=" + raw.appId
                    + ",logId=" + raw.logId + ",common=" + raw.common + ",events=" + raw.events
                    + ",ts=" + raw.ts + ",dt=" + raw.dt + "}";
        }
    }

    private static String normalizeEvent(String value) {
        String normalized = value.toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "delivery", "show", "click", "convert" -> normalized;
            default -> throw new IllegalArgumentException("unsupported event: " + value);
        };
    }

    private static long requiredLong(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull() || !value.canConvertToLong()) {
            throw new IllegalArgumentException("missing or invalid field: " + field);
        }
        return value.asLong();
    }

    private static String requiredText(JsonNode node, String field) {
        String value = optionalText(node, field);
        if (value.isBlank()) throw new IllegalArgumentException("missing field: " + field);
        return value;
    }

    private static String optionalText(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? "" : value.asText("");
    }
}
