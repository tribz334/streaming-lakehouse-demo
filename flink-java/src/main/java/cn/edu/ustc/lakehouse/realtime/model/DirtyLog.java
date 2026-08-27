package cn.edu.ustc.lakehouse.realtime.model;

import java.io.Serializable;

/** Dirty ODS event compatible with dwd_ad_event_dirty_di. */
public class DirtyLog implements Serializable {
    public long eventId;
    public Long creativeId;
    public String errorReason;
    public String common;
    public String events;
    public long ts;
    public String dt;

    public DirtyLog() {}
}
