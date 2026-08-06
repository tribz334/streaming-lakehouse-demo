package cn.edu.ustc.lakehouse.realtime.model;

import java.io.Serializable;

/** Valid page-view/page-leave fact split from the shared ods_log stream. */
public class PageLogEvent implements Serializable {
    private String eventId;
    private long eventTimeMillis;
    private String userId;
    private String eventType;
    private String pageId;
    private String lastPageId;
    private long durationMillis;
    private String deviceId;
    private String source;

    public PageLogEvent() {}

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    public long getEventTimeMillis() { return eventTimeMillis; }
    public void setEventTimeMillis(long eventTimeMillis) { this.eventTimeMillis = eventTimeMillis; }
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }
    public String getPageId() { return pageId; }
    public void setPageId(String pageId) { this.pageId = pageId; }
    public String getLastPageId() { return lastPageId; }
    public void setLastPageId(String lastPageId) { this.lastPageId = lastPageId; }
    public long getDurationMillis() { return durationMillis; }
    public void setDurationMillis(long durationMillis) { this.durationMillis = durationMillis; }
    public String getDeviceId() { return deviceId; }
    public void setDeviceId(String deviceId) { this.deviceId = deviceId; }
    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }
}
