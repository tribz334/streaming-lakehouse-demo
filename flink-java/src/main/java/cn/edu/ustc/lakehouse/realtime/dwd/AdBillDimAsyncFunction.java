package cn.edu.ustc.lakehouse.realtime.dwd;

import cn.edu.ustc.lakehouse.realtime.config.RealtimeJobConfig;
import cn.edu.ustc.lakehouse.realtime.model.AdBill;
import cn.edu.ustc.lakehouse.realtime.model.DimCreative;

/** Adds the creative dimension's closed-loop flag to an atomic bill fact. */
public final class AdBillDimAsyncFunction
        extends BaseAsyncDimFunction<AdBill, Long, DimCreative> {
    public AdBillDimAsyncFunction(RealtimeJobConfig config) {
        super(config);
    }

    @Override
    protected Long getKey(AdBill bill) {
        return bill.creativeId;
    }

    @Override
    protected DimCreative newDimension() {
        return new DimCreative();
    }

    @Override
    protected void enrich(AdBill bill, DimCreative creative) {
        bill.isClosed = creative.isClosed;
    }
}
