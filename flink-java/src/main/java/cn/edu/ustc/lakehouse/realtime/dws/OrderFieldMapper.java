package cn.edu.ustc.lakehouse.realtime.dws;

import cn.edu.ustc.lakehouse.realtime.model.CreativeField;
import org.apache.flink.types.Row;

/** Maps PAY/REFUND changelog events to source-neutral creative metric fields. */
public final class OrderFieldMapper extends BaseFieldMapper<Row> {
    @Override
    public CreativeField map(Row row) {
        int sign = sign(row.getKind());
        CreativeField field = initField(
                requiredLong(row, 0), requiredLong(row, 5), string(row, 6), row.getKind());
        field.placementType = nullableInt(row, 3);
        field.adType = nullableInt(row, 4);
        long amount = requiredLong(row, 2) * sign;
        if ("PAY".equals(string(row, 1))) {
            field.payOrderCount = sign;
            field.payOrderGmv = amount;
        } else if ("REFUND".equals(string(row, 1))) {
            field.refundOrderCount = sign;
            field.refundOrderGmv = amount;
        }
        return field;
    }
}
