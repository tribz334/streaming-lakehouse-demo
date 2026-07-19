package cn.edu.ustc.lakehouse.realtime;

import java.util.Map;

public final class AdvertiserDimAsyncFunction
        extends DimAsyncFunction<RealtimeMetric> {

    public AdvertiserDimAsyncFunction(RealtimeJobConfig config) {
        super(config);
    }

    @Override
    protected String getKey(RealtimeMetric metric) {
        return metric.getAdvertiserId();
    }

    @Override
    protected String getTableName() {
        return "advertiser";
    }

    @Override
    protected void join(RealtimeMetric metric, Map<String, Object> dim) {
        Object advertiserName = dim.get("advertiser_name");
        metric.setAdvertiserName(advertiserName == null ? "UNKNOWN" : advertiserName.toString());
    }
}
