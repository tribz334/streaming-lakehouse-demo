package cn.edu.ustc.lakehouse.realtime.model;

import cn.edu.ustc.lakehouse.realtime.config.FlussTableNames;
import org.apache.fluss.row.InternalRow;

/** Core creative hierarchy attributes reused by DWD fact enrichment. */
public class DimCreative implements BaseDim<Long> {
    public Long creativeId;
    public byte isClosed;
    public Long unitId;
    public Long campaignId;
    public Long advertiserId;
    public int adType;
    public int placementType;

    public DimCreative() {}

    @Override
    public String getTableName() {
        return FlussTableNames.DIM_CREATIVE;
    }

    @Override
    public Long getKey() {
        return creativeId;
    }

    @Override
    public void fromRow(InternalRow row) {
        creativeId = nullableLong(row, 0);
        unitId = nullableLong(row, 2);
        campaignId = nullableLong(row, 4);
        advertiserId = nullableLong(row, 5);
        adType = nullableInt(row, 6);
        placementType = nullableInt(row, 7);
        isClosed = row.isNullAt(8) ? 0 : row.getByte(8);
    }

    private static Long nullableLong(InternalRow row, int position) {
        return row.isNullAt(position) ? null : row.getLong(position);
    }

    private static int nullableInt(InternalRow row, int position) {
        return row.isNullAt(position) ? 0 : row.getInt(position);
    }
}
