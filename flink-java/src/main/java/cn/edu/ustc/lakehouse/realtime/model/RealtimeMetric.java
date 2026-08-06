package cn.edu.ustc.lakehouse.realtime.model;

import cn.edu.ustc.lakehouse.realtime.dws.DwsMetricAggregation;
import cn.edu.ustc.lakehouse.realtime.dws.MetricKey;

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
    private String campaignId;
    private String unitId;
    private String creativeId;
    private String media;
    private String commerceScene;
    private BigDecimal spend;
    private BigDecimal orderGmv;
    private BigDecimal attributedGmv;
    private BigDecimal organicGmv;
    private long impressions;
    private long clicks;
    private long paidOrders;
    private long attributedOrders;
    private long organicOrders;
    private BigDecimal ctr;
    private BigDecimal cvr;
    private BigDecimal roi;
    private LocalDateTime updatedAt;

    public RealtimeMetric() {}

    public static RealtimeMetric from(
            long windowStartMillis, long windowEndMillis,
            MetricKey key, DwsMetricAggregation.Accumulator accumulator) {
        RealtimeMetric metric = new RealtimeMetric();
        metric.windowStart = toLocalDateTime(windowStartMillis);
        metric.windowEnd = toLocalDateTime(windowEndMillis);
        metric.advertiserId = key.getAdvertiserId();
        metric.campaignId = key.getCampaignId();
        metric.unitId = key.getUnitId();
        metric.creativeId = key.getCreativeId();
        metric.media = key.getMedia();
        metric.commerceScene = key.getCommerceScene();
        metric.spend = accumulator.getSpend().setScale(4, RoundingMode.HALF_UP);
        metric.orderGmv = accumulator.getOrderGmv().setScale(2, RoundingMode.HALF_UP);
        metric.attributedGmv = accumulator.getAttributedGmv().setScale(2, RoundingMode.HALF_UP);
        metric.organicGmv = accumulator.getOrganicGmv().setScale(2, RoundingMode.HALF_UP);
        metric.impressions = accumulator.getImpressions();
        metric.clicks = accumulator.getClicks();
        metric.paidOrders = accumulator.getPaidOrders();
        metric.attributedOrders = accumulator.getAttributedOrders();
        metric.organicOrders = accumulator.getOrganicOrders();
        metric.ctr = divide(metric.clicks, metric.impressions);
        metric.cvr = divide(metric.paidOrders, metric.clicks);
        metric.roi = divide(metric.attributedGmv, metric.spend);
        metric.updatedAt = LocalDateTime.now(BUSINESS_ZONE);
        return metric;
    }

    private static LocalDateTime toLocalDateTime(long millis) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), BUSINESS_ZONE);
    }

    private static BigDecimal divide(long numerator, long denominator) {
        if (denominator == 0) return null;
        return BigDecimal.valueOf(numerator).divide(
                BigDecimal.valueOf(denominator), 6, RoundingMode.HALF_UP);
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
    public String getCampaignId() { return campaignId; }
    public void setCampaignId(String campaignId) { this.campaignId = campaignId; }
    public String getUnitId() { return unitId; }
    public void setUnitId(String unitId) { this.unitId = unitId; }
    public String getCreativeId() { return creativeId; }
    public void setCreativeId(String creativeId) { this.creativeId = creativeId; }
    public String getMedia() { return media; }
    public void setMedia(String media) { this.media = media; }
    public String getCommerceScene() { return commerceScene; }
    public void setCommerceScene(String commerceScene) { this.commerceScene = commerceScene; }
    public BigDecimal getSpend() { return spend; }
    public void setSpend(BigDecimal spend) { this.spend = spend; }
    public BigDecimal getOrderGmv() { return orderGmv; }
    public void setOrderGmv(BigDecimal orderGmv) { this.orderGmv = orderGmv; }
    public BigDecimal getAttributedGmv() { return attributedGmv; }
    public void setAttributedGmv(BigDecimal attributedGmv) { this.attributedGmv = attributedGmv; }
    public BigDecimal getOrganicGmv() { return organicGmv; }
    public void setOrganicGmv(BigDecimal organicGmv) { this.organicGmv = organicGmv; }
    public long getImpressions() { return impressions; }
    public void setImpressions(long impressions) { this.impressions = impressions; }
    public long getClicks() { return clicks; }
    public void setClicks(long clicks) { this.clicks = clicks; }
    public long getPaidOrders() { return paidOrders; }
    public void setPaidOrders(long paidOrders) { this.paidOrders = paidOrders; }
    public long getAttributedOrders() { return attributedOrders; }
    public void setAttributedOrders(long attributedOrders) { this.attributedOrders = attributedOrders; }
    public long getOrganicOrders() { return organicOrders; }
    public void setOrganicOrders(long organicOrders) { this.organicOrders = organicOrders; }
    public BigDecimal getCtr() { return ctr; }
    public void setCtr(BigDecimal ctr) { this.ctr = ctr; }
    public BigDecimal getCvr() { return cvr; }
    public void setCvr(BigDecimal cvr) { this.cvr = cvr; }
    public BigDecimal getRoi() { return roi; }
    public void setRoi(BigDecimal roi) { this.roi = roi; }
    public LocalDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(LocalDateTime updatedAt) { this.updatedAt = updatedAt; }
}
