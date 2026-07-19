package cn.edu.ustc.lakehouse.realtime;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class RealtimeMetric implements Serializable {
    private static final ZoneId BUSINESS_ZONE = ZoneId.of("Asia/Shanghai");

    private LocalDateTime windowStart;
    private LocalDateTime windowEnd;
    private String advertiserId;
    private String advertiserName;
    private String campaignId;
    private String unitId;
    private String creativeId;
    private BigDecimal spend;
    private BigDecimal gmv;
    private long impressions;
    private long clicks;
    private long conversions;
    private long orders;
    private BigDecimal ctr;
    private BigDecimal cvr;
    private BigDecimal roi;
    private LocalDateTime updatedAt;

    public RealtimeMetric() {}

    public static RealtimeMetric from(
            long windowStartMillis, long windowEndMillis,
            MetricKey key, MetricAccumulator accumulator) {
        RealtimeMetric metric = new RealtimeMetric();
        metric.windowStart = toLocalDateTime(windowStartMillis);
        metric.windowEnd = toLocalDateTime(windowEndMillis);
        metric.advertiserId = key.getAdvertiserId();
        metric.campaignId = key.getCampaignId();
        metric.unitId = key.getUnitId();
        metric.creativeId = key.getCreativeId();
        metric.spend = accumulator.getSpend().setScale(4, RoundingMode.HALF_UP);
        metric.gmv = accumulator.getGmv().setScale(2, RoundingMode.HALF_UP);
        metric.impressions = accumulator.getImpressions();
        metric.clicks = accumulator.getClicks();
        metric.conversions = accumulator.getConversions();
        metric.orders = accumulator.getOrders();
        metric.ctr = divide(metric.clicks, metric.impressions);
        metric.cvr = divide(metric.conversions, metric.clicks);
        metric.roi = divide(metric.gmv, metric.spend);
        metric.updatedAt = LocalDateTime.now(BUSINESS_ZONE);
        return metric;
    }

    private static LocalDateTime toLocalDateTime(long millis) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), BUSINESS_ZONE);
    }

    private static BigDecimal divide(long numerator, long denominator) {
        if (denominator == 0) return null;
        return BigDecimal.valueOf(numerator).divide(BigDecimal.valueOf(denominator), 6, RoundingMode.HALF_UP);
    }

    private static BigDecimal divide(BigDecimal numerator, BigDecimal denominator) {
        if (denominator == null || denominator.signum() == 0) return null;
        return numerator.divide(denominator, 6, RoundingMode.HALF_UP);
    }

    public LocalDateTime getWindowStart() { return windowStart; }
    public void setWindowStart(LocalDateTime windowStart) { this.windowStart = windowStart; }
    public LocalDateTime getWindowEnd() { return windowEnd; }
    public void setWindowEnd(LocalDateTime windowEnd) { this.windowEnd = windowEnd; }
    public String getAdvertiserId() { return advertiserId; }
    public void setAdvertiserId(String advertiserId) { this.advertiserId = advertiserId; }
    public String getAdvertiserName() { return advertiserName; }
    public void setAdvertiserName(String advertiserName) { this.advertiserName = advertiserName; }
    public String getCampaignId() { return campaignId; }
    public void setCampaignId(String campaignId) { this.campaignId = campaignId; }
    public String getUnitId() { return unitId; }
    public void setUnitId(String unitId) { this.unitId = unitId; }
    public String getCreativeId() { return creativeId; }
    public void setCreativeId(String creativeId) { this.creativeId = creativeId; }
    public BigDecimal getSpend() { return spend; }
    public void setSpend(BigDecimal spend) { this.spend = spend; }
    public BigDecimal getGmv() { return gmv; }
    public void setGmv(BigDecimal gmv) { this.gmv = gmv; }
    public long getImpressions() { return impressions; }
    public void setImpressions(long impressions) { this.impressions = impressions; }
    public long getClicks() { return clicks; }
    public void setClicks(long clicks) { this.clicks = clicks; }
    public long getConversions() { return conversions; }
    public void setConversions(long conversions) { this.conversions = conversions; }
    public long getOrders() { return orders; }
    public void setOrders(long orders) { this.orders = orders; }
    public BigDecimal getCtr() { return ctr; }
    public void setCtr(BigDecimal ctr) { this.ctr = ctr; }
    public BigDecimal getCvr() { return cvr; }
    public void setCvr(BigDecimal cvr) { this.cvr = cvr; }
    public BigDecimal getRoi() { return roi; }
    public void setRoi(BigDecimal roi) { this.roi = roi; }
    public LocalDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(LocalDateTime updatedAt) { this.updatedAt = updatedAt; }
}
