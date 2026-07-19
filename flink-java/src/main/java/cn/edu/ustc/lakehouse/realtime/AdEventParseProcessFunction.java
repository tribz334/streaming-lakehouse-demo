package cn.edu.ustc.lakehouse.realtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

public class AdEventParseProcessFunction extends ProcessFunction<String, AdEvent> {
    private final OutputTag<String> dirtyOutputTag;
    private transient ObjectMapper objectMapper;

    public AdEventParseProcessFunction(OutputTag<String> dirtyOutputTag) {
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
            String eventId = requiredText(node, "event_id");
            long eventTimeMillis = OffsetDateTime.parse(requiredText(node, "ts")).toInstant().toEpochMilli();
            AdEvent event = new AdEvent(
                    eventId,
                    eventTimeMillis,
                    requiredText(node, "advertiser_id"),
                    requiredText(node, "campaign_id"),
                    requiredText(node, "unit_id"),
                    requiredText(node, "creative_id"),
                    requiredText(node, "event_type"),
                    decimal(node, "spend"),
                    decimal(node, "gmv"));
            output.collect(event);
        } catch (Exception parseError) {
            context.output(dirtyOutputTag, value);
        }
    }

    private static String requiredText(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull() || value.asText().isBlank()) {
            throw new IllegalArgumentException("missing field: " + field);
        }
        return value.asText();
    }

    private static BigDecimal decimal(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? BigDecimal.ZERO : value.decimalValue();
    }
}
