package cn.edu.ustc.lakehouse.realtime.dwd;

import cn.edu.ustc.lakehouse.realtime.model.AdClickEvent;
import cn.edu.ustc.lakehouse.realtime.model.OrderDetail;

import java.io.Serializable;
import java.util.Objects;

/**
 * Business key shared by the click and order streams.
 *
 * <p>Matches section 4.3.4 exactly: {@code product_id + user_id}. Advertiser is
 * deliberately not part of the key because product is the order-side business
 * entity being attributed.</p>
 */
public class AttributionKey implements Serializable {
    private String userId;
    private String productId;

    public AttributionKey() {}

    public AttributionKey(String userId, String productId) {
        this.userId = userId;
        this.productId = productId;
    }

    public static AttributionKey from(AdClickEvent click) {
        return new AttributionKey(click.getUserId(), click.getProductId());
    }

    public static AttributionKey from(OrderDetail order) {
        return new AttributionKey(order.getUserId(), order.getProductId());
    }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    public String getProductId() { return productId; }
    public void setProductId(String productId) { this.productId = productId; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof AttributionKey key)) return false;
        return Objects.equals(userId, key.userId)
                && Objects.equals(productId, key.productId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, productId);
    }

    @Override
    public String toString() {
        return productId + "|" + userId;
    }
}
