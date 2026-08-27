package cn.edu.ustc.lakehouse.realtime.model;

import java.io.Serializable;

/** One append-only row from Fluss ods_log_di. */
public class RawLog implements Serializable {
    public long msgId;
    public int busId;
    public int appId;
    public int logId;
    public String common;
    public String events;
    public long ts;
    public String dt;

    public RawLog() {}
}
