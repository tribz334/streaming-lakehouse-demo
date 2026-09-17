package cn.edu.ustc.lakehouse.realtime.dws;

import cn.edu.ustc.lakehouse.realtime.model.CreativeField;
import cn.edu.ustc.lakehouse.realtime.model.DimCreative;
import cn.edu.ustc.lakehouse.realtime.model.MetricAccumulator;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FieldMapperTest {
    @Test
    void mapsAdEventWithoutDimensionLookup() throws Exception {
        Row row = Row.ofKind(RowKind.INSERT, 101L, "click", 2, 1, 1_000L, "2026-09-02");

        CreativeField field = new AdEventFieldMapper().map(row);

        assertEquals(101L, field.creativeId);
        assertEquals(1L, field.clickCount);
        assertEquals(0L, field.cost);
    }

    @Test
    void mapsBillAndCalculatesClosedCostFromCreativeDimension() throws Exception {
        Row row = Row.ofKind(RowKind.INSERT,
                101L, 880L, 101L, (byte) 1, 11L, 12L, 13L, "1", "2",
                2_000L, "2026-09-02");

        CreativeField field = new BillFieldMapper().map(row);

        assertEquals(880L, field.cost);
        assertEquals(880L, field.closedCost);
        assertEquals(0L, BillFieldMapper.calcClosedCost(880L, (byte) 0));
    }

    @Test
    void mapsOrderWithoutDimensionLookup() throws Exception {
        Row row = Row.ofKind(RowKind.INSERT,
                101L, "PAY", 9_900L, 2, 1, 3_000L, "2026-09-02");

        CreativeField field = new OrderFieldMapper().map(row);

        assertEquals(1L, field.payOrderCount);
        assertEquals(9_900L, field.payOrderGmv);
        assertEquals(0L, field.refundOrderCount);
    }

    @Test
    void creativeDimensionMapsLookupRowAndImplementsContract() {
        Row row = Row.ofKind(RowKind.INSERT,
                101L, 880L, 101L, (byte) 1, 11L, 12L, 13L, "1", "2",
                2_000L, "2026-09-02");
        DimCreative dim = new DimCreative();

        dim.fromRow(row);

        assertEquals("dim_creative", dim.getTableName());
        assertEquals(101L, dim.getKey());
        assertEquals(1, dim.isClosed);
    }

    @Test
    void aggregatorKeepsExistingMetricDefinitions() {
        CreativeField field = new CreativeField();
        field.dt = "2026-09-02";
        field.payOrderCount = 1;
        field.payOrderGmv = 9_900L;
        field.adType = 1;
        field.placementType = 2;
        CreativeSummaryAggregator.Incremental aggregate =
                new CreativeSummaryAggregator.Incremental();

        MetricAccumulator result = aggregate.add(field, aggregate.createAccumulator());

        assertEquals(1L, result.payOrderCount);
        assertEquals(9_900L, result.payOrderGmv);
        assertEquals(9_900L, result.shortVideoPayOrderGmv);
        assertEquals(9_900L, result.searchPayOrderGmv);
    }
}
