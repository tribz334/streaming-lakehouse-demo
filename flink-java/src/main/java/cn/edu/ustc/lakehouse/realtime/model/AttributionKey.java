package cn.edu.ustc.lakehouse.realtime.model;

import java.io.Serializable;
import java.util.Objects;

/** LastClick join key: uid + product_id. */
public class AttributionKey implements Serializable {
    public long uid;
    public long productId;

    public AttributionKey() {}
    public AttributionKey(long uid, long productId) { this.uid = uid; this.productId = productId; }

    @Override public boolean equals(Object value) {
        if (this == value) return true;
        if (!(value instanceof AttributionKey that)) return false;
        return uid == that.uid && productId == that.productId;
    }
    @Override public int hashCode() { return Objects.hash(uid, productId); }
}
