package cn.edu.ustc.lakehouse.realtime.model;

import org.apache.fluss.row.InternalRow;

import java.io.Serializable;

/** Common contract for Fluss primary-key dimensions and row-to-domain mapping. */
public interface BaseDim<K> extends Serializable {
    String getTableName();
    K getKey();
    void fromRow(InternalRow row);
}
