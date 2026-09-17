package cn.edu.ustc.lakehouse.realtime.model;

import java.io.Serializable;
/** Last Click result enriched with core DIM attributes before writing dwd_order_acc. */
public class AttributedOrder implements Serializable {
    public long orderId;
    public long uid;
    public long productId;
    public Long shopId;
    public Long creativeId;
    public Long unitId;
    public Long campaignId;
    public Long advertiserId;
    public byte isClosed;
    public int adType;
    public int placementType;
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
    public String hour;

    public AttributedOrder() {}
}
