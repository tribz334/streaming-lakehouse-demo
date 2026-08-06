package cn.edu.ustc.lakehouse.realtime.dwd;

import cn.edu.ustc.lakehouse.realtime.model.AdBill;
import cn.edu.ustc.lakehouse.realtime.model.AdEvent;

import org.apache.flink.streaming.api.datastream.DataStream;

import java.math.BigDecimal;

/** Converts authoritative ad bill records into the common DWS fact contract. */
public final class DwdAdBill {
    private DwdAdBill() {}

    public static DataStream<AdEvent> build(DataStream<AdBill> bills) {
        return bills
                .map(DwdAdBill::toAdEvent)
                .returns(AdEvent.class)
                .name("DwdAdBill: map advertising charges to DWD fact events");
    }

    private static AdEvent toAdEvent(AdBill bill) {
        AdEvent event = new AdEvent();
        event.setEventId("bill-" + bill.getBillId());
        event.setEventTimeMillis(bill.getBillTimeMillis());
        event.setAdvertiserId(bill.getAdvertiserId());
        event.setCampaignId(bill.getCampaignId());
        event.setUnitId(bill.getUnitId());
        event.setCreativeId(bill.getCreativeId());
        event.setUserId(bill.getUserId());
        event.setMedia(bill.getMedia());
        event.setCommerceScene(bill.getCommerceScene());
        event.setSpend(bill.getCost());
        event.setEventType("bill");
        event.setOrderGmv(BigDecimal.ZERO);
        event.setAttributedGmv(BigDecimal.ZERO);
        event.setOrganicGmv(BigDecimal.ZERO);
        return event;
    }
}
