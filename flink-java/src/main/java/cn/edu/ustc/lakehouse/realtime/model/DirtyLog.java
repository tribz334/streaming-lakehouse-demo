package cn.edu.ustc.lakehouse.realtime.model;

import java.io.Serializable;

/** Original log payload that could not be parsed or validated. */
public class DirtyLog implements Serializable {
    public String rawData;
    public String errorReason;
    public long errorTime;
    public String dt;

    public DirtyLog() {}
}
