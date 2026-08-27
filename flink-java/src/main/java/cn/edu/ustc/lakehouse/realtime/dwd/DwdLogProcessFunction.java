package cn.edu.ustc.lakehouse.realtime.dwd;

import cn.edu.ustc.lakehouse.realtime.model.DirtyLog;
import cn.edu.ustc.lakehouse.realtime.model.ParsedAdEvent;
import cn.edu.ustc.lakehouse.realtime.model.RawLog;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Locale;

/** Parses, validates and fans out the JSON payload retained by Fluss ods_log_di. */
public final class DwdLogProcessFunction extends ProcessFunction<RawLog, ParsedAdEvent> {
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
    public void processElement(RawLog raw, Context context, Collector<ParsedAdEvent> output) {
        try {
            validateEnvelope(raw);
            JsonNode common = parseJson(raw.common, "common");
            JsonNode actions = parseJson(raw.events, "events");
            if (!actions.isArray() || actions.isEmpty()) {
                throw new IllegalArgumentException("events must be a non-empty JSON array");
            }
            processActions(raw, common, actions, output);
        } catch (Exception error) {
            context.output(dirtyOutput, collectDirty(raw, error));
        }
    }

    private void processActions(
            RawLog raw, JsonNode common, JsonNode actions, Collector<ParsedAdEvent> output) {
        for (int index = 0; index < actions.size(); index++) {
            output.collect(parseAction(raw, common, actions.get(index), index));
        }
    }

    private ParsedAdEvent parseAction(RawLog raw, JsonNode common, JsonNode action, int index) {
        ParsedAdEvent event = new ParsedAdEvent();
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

    private void validateEnvelope(RawLog raw) {
        if (raw.msgId <= 0 || raw.busId <= 0 || raw.appId <= 0 || raw.logId <= 0 || raw.ts <= 0) {
            throw new IllegalArgumentException("invalid SDK envelope identifier or timestamp");
        }
        if (raw.dt == null || raw.dt.isBlank()) {
            throw new IllegalArgumentException("dt is empty");
        }
    }

    private DirtyLog collectDirty(RawLog raw, Exception error) {
        DirtyLog dirty = new DirtyLog();
        dirty.eventId = raw.msgId;
        dirty.creativeId = extractCreativeId(raw.events);
        dirty.errorReason = "PARSE_OR_VALIDATION_ERROR: " + error.getMessage();
        dirty.common = raw.common;
        dirty.events = raw.events;
        dirty.ts = raw.ts;
        dirty.dt = raw.dt;
        return dirty;
    }

    private Long extractCreativeId(String events) {
        try {
            JsonNode actions = mapper.readTree(events);
            return actions.isArray() && !actions.isEmpty() && actions.get(0).has("creative_id")
                    ? actions.get(0).get("creative_id").asLong()
                    : null;
        } catch (Exception ignored) {
            return null;
        }
    }

    private static String normalizeEvent(String value) {
        String normalized = value.toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "send" -> "delivery";
            case "show" -> "impression";
            case "convert" -> "conversion";
            case "delivery", "impression", "click", "conversion" -> normalized;
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
