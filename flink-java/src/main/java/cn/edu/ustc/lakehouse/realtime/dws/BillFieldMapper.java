package cn.edu.ustc.lakehouse.realtime.dws;

import cn.edu.ustc.lakehouse.realtime.config.RealtimeJobConfig;
import cn.edu.ustc.lakehouse.realtime.model.CreativeField;
import cn.edu.ustc.lakehouse.realtime.config.FlussTableNames;
import cn.edu.ustc.lakehouse.realtime.util.FlussUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

/** Maps DWD bill facts and derives closed cost only at the DWS aggregation boundary. */
public final class BillFieldMapper extends BaseFieldMapper<Row> {
    public static Table readBills(
            FlussUtil.Context context, RealtimeJobConfig config) {
        String database = config.flussDatabase();
        return FlussUtil.lookup(context,
                "SELECT b.creative_id,b.cost,b.is_closed,b.ts,"
                        + "CONCAT(SUBSTRING(b.dt,1,4),'-',SUBSTRING(b.dt,5,2),'-',"
                        + "SUBSTRING(b.dt,7,2)) FROM fluss." + database
                        + "." + FlussTableNames.DWD_AD_BILL + FlussUtil.scanHint(config) + " b");
    }

    @Override
    public CreativeField map(Row row) {
        int sign = sign(row.getKind());
        CreativeField field = initField(
                requiredLong(row, 0), requiredLong(row, 3), string(row, 4), row.getKind());
        field.cost = requiredLong(row, 1) * sign;
        field.closedCost = calcClosedCost(field.cost, (byte) nullableInt(row, 2));
        return field;
    }

    static long calcClosedCost(long cost, byte isClosed) {
        return isClosed == 1 ? cost : 0L;
    }
}
