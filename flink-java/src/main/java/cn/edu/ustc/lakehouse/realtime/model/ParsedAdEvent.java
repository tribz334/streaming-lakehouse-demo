package cn.edu.ustc.lakehouse.realtime.model;

import java.io.Serializable;

/** Validated SDK event before DIM enrichment. */
public class ParsedAdEvent implements Serializable {
    public long eventId;
    public long uid;
    public String deviceId;
    public int platform;
    public String appVc;
    public String browserVc;
    public String sdkVc;
    public long creativeId;
    public long productId;
    public long slotId;
    public String eventType;
    public long ts;
    public String dt;

    public ParsedAdEvent() {}
}
