package cn.edu.ustc.lakehouse.realtime.model;

import java.io.Serializable;

/** Paid order snapshot carried by the order side of the connected stream. */
public class OrderInfo implements Serializable {
    public long orderId;
    public long uid;
    public long productId;
    public Long shopId;
    public long productPrice;
    public int productNum;
    public long totalAmount;
    public Integer paymentMethod;
    public String receiverName;
    public String receiverPhone;
    public String shippingAddress;
    public String trackingNumber;
    public int orderStatus;
    public String createTime;
    public String cancelTime;
    public String payTime;
    public String confirmTime;
    public String refundTime;
    public String updatedAt;
    public String dt;
    public long payTimeMillis;
    public long eventTimeMillis;
    /** One event-time timer per pending order; part of managed state. */
    public long attributionTimerMillis;

    public OrderInfo() {}

    public AttributionKey key() {
        return new AttributionKey(uid, productId);
    }
}
