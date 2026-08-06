package cn.edu.ustc.lakehouse.realtime.dwd;

import cn.edu.ustc.lakehouse.realtime.model.AdEvent;
import cn.edu.ustc.lakehouse.realtime.model.CreativeDimChange;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/** Enriches SDK behavior facts from the CDC-maintained creative hierarchy. */
public final class DimBroadcastEnrichment
        extends BroadcastProcessFunction<AdEvent, CreativeDimChange, AdEvent> {
    public static final MapStateDescriptor<String, CreativeDimChange> DIM_STATE =
            new MapStateDescriptor<>(
                    "creative-dim-broadcast-state", String.class, CreativeDimChange.class);

    private final OutputTag<String> dirtyOutputTag;
    private transient ObjectMapper objectMapper;

    public DimBroadcastEnrichment(OutputTag<String> dirtyOutputTag) {
        this.dirtyOutputTag = dirtyOutputTag;
    }

    @Override
    public void open(OpenContext openContext) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public void processBroadcastElement(
            CreativeDimChange change,
            Context context,
            Collector<AdEvent> output) throws Exception {
        BroadcastState<String, CreativeDimChange> state =
                context.getBroadcastState(DIM_STATE);
        if (change.isDelete()) state.remove(change.getCreativeId());
        else state.put(change.getCreativeId(), change);
    }

    @Override
    public void processElement(
            AdEvent event,
            ReadOnlyContext context,
            Collector<AdEvent> output) throws Exception {
        ReadOnlyBroadcastState<String, CreativeDimChange> state =
                context.getBroadcastState(DIM_STATE);
        CreativeDimChange dimension = state.get(event.getCreativeId());
        if (dimension == null
                || blank(dimension.getAdvertiserId())
                || blank(dimension.getCampaignId())
                || blank(dimension.getUnitId())) {
            context.output(
                    dirtyOutputTag,
                    objectMapper.createObjectNode()
                            .put("event_id", event.getEventId())
                            .put("creative_id", event.getCreativeId())
                            .put("error_message", "creative DIM hierarchy not found")
                            .put("dirty_time", System.currentTimeMillis())
                            .toString());
            return;
        }

        event.setAdvertiserId(dimension.getAdvertiserId());
        event.setCampaignId(dimension.getCampaignId());
        event.setUnitId(dimension.getUnitId());
        output.collect(event);
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }
}
