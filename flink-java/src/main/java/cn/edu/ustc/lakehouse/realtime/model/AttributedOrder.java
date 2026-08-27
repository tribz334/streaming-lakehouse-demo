package cn.edu.ustc.lakehouse.realtime.model;

import java.io.Serializable;
import java.time.Instant;

/** Final LastClick result written to Fluss dwd_ad_order_acc. */
public class AttributedOrder implements Serializable {
    public long orderId;
    public long uid;
    public long productId;
    public long shopId;
    public Long creativeId;
    public Long slotId;
    public long productPrice;
    public int productNum;
    public long totalAmount;
    public int paymentMethod;
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
    public Long advertiserId;
    public Long campaignId;
    public Long unitId;
    public Integer placementType;
    public Integer adType;
    public Instant clickTime;
    public boolean directAttribution;
    public Instant eventTime;
    public String dt;
    public String hour;

    public AttributedOrder() {}
}
