package cn.edu.ustc.lakehouse.realtime.dwd;

import cn.edu.ustc.lakehouse.realtime.config.RealtimeJobConfig;
import cn.edu.ustc.lakehouse.realtime.model.AttributedOrder;
import cn.edu.ustc.lakehouse.realtime.model.DimCreative;

/** Enriches an attributed order with frequently used creative hierarchy attributes. */
public final class AttributedOrderDimAsyncFunction
        extends BaseAsyncDimFunction<AttributedOrder, Long, DimCreative> {
    public AttributedOrderDimAsyncFunction(RealtimeJobConfig config) {
        super(config);
    }

    @Override
    protected Long getKey(AttributedOrder order) {
        return order.creativeId;
    }

    @Override
    protected DimCreative newDimension() {
        return new DimCreative();
    }

    @Override
    protected void enrich(AttributedOrder order, DimCreative creative) {
        order.unitId = creative.unitId;
        order.campaignId = creative.campaignId;
        order.advertiserId = creative.advertiserId;
        order.isClosed = creative.isClosed;
        order.adType = creative.adType;
        order.placementType = creative.placementType;
    }
}
