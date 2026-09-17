package cn.edu.ustc.lakehouse.realtime.dws;

import cn.edu.ustc.lakehouse.realtime.model.CreativeField;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

/** Shared initialization and conversion helpers for source-specific field mappers. */
public abstract class BaseFieldMapper<T> implements MapFunction<T, CreativeField> {
    protected CreativeField initField(
            long creativeId, long eventTimeMillis, String dt, RowKind kind) {
        CreativeField field = new CreativeField();
        fillCommonFields(field, creativeId, eventTimeMillis, dt);
        return field;
    }

    protected void fillCommonFields(
            CreativeField field, long creativeId, long eventTimeMillis, String dt) {
        field.creativeId = creativeId;
        field.eventTimeMillis = eventTimeMillis;
        field.dt = dt;
    }

    protected static int sign(RowKind kind) {
        return kind == RowKind.UPDATE_BEFORE || kind == RowKind.DELETE ? -1 : 1;
    }

    protected static long requiredLong(Row row, int position) {
        Object value = row.getField(position);
        if (value instanceof Number number) return number.longValue();
        throw new IllegalArgumentException(
                "Expected numeric field at position " + position + ": " + value);
    }

    protected static int nullableInt(Row row, int position) {
        Object value = row.getField(position);
        if (value instanceof Number number) return number.intValue();
        if (value == null) return 0;
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException ignored) {
            return 0;
        }
    }

    protected static String string(Row row, int position) {
        Object value = row.getField(position);
        return value == null ? null : value.toString();
    }
}
