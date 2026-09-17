package cn.edu.ustc.lakehouse.realtime.dws;

import cn.edu.ustc.lakehouse.realtime.model.CreativeField;
import org.apache.flink.types.Row;

/** Maps advertising events to source-neutral creative metric fields. */
public final class AdEventFieldMapper extends BaseFieldMapper<Row> {
    /** Ignore event kinds that do not contribute to these four counters. */
    public static boolean isMetricEvent(Row row) {
        String type = string(row, 1);
        return "delivery".equals(type) || "show".equals(type)
                || "click".equals(type) || "convert".equals(type);
    }

    @Override
    public CreativeField map(Row row) {
        int sign = sign(row.getKind());
        CreativeField field = initField(
                requiredLong(row, 0), requiredLong(row, 4), string(row, 5), row.getKind());
        field.placementType = nullableInt(row, 2);
        field.adType = nullableInt(row, 3);
        String eventType = string(row, 1);
        if ("delivery".equals(eventType)) field.deliveryCount = sign;
        else if ("show".equals(eventType)) field.impressionCount = sign;
        else if ("click".equals(eventType)) field.clickCount = sign;
        else if ("convert".equals(eventType)) field.conversionCount = sign;
        return field;
    }
}
