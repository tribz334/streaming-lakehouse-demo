package cn.edu.ustc.lakehouse.realtime;

import java.io.Serializable;
import java.math.BigDecimal;

public class MetricAccumulator implements Serializable {
    private BigDecimal spend = BigDecimal.ZERO;
    private BigDecimal gmv = BigDecimal.ZERO;
    private long impressions;
    private long clicks;
    private long conversions;
    private long orders;

    public MetricAccumulator add(AdEvent event) {
        spend = spend.add(orZero(event.getSpend()));
        gmv = gmv.add(orZero(event.getGmv()));
        switch (event.getEventType()) {
            case "impression" -> impressions++;
            case "click" -> clicks++;
            case "conversion" -> conversions++;
            case "order" -> orders++;
            default -> { }
        }
        return this;
    }

    public MetricAccumulator merge(MetricAccumulator other) {
        spend = spend.add(other.spend);
        gmv = gmv.add(other.gmv);
        impressions += other.impressions;
        clicks += other.clicks;
        conversions += other.conversions;
        orders += other.orders;
        return this;
    }

    private static BigDecimal orZero(BigDecimal value) {
        return value == null ? BigDecimal.ZERO : value;
    }

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
}
