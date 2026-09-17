package cn.edu.ustc.lakehouse.realtime.model;

import java.io.Serializable;

/** Validated and standardized advertising action parsed from an SDK log. */
public class AdEvent implements Serializable {
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

    public AdEvent() {}
}
