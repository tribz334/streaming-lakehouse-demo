package cn.edu.ustc.lakehouse.realtime.dwd;

import cn.edu.ustc.lakehouse.realtime.model.AdClickEvent;
import cn.edu.ustc.lakehouse.realtime.model.OrderDetail;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/** Paper-aligned connected-stream implementation of dwd_order_detail. */
public final class DwdOrderDetail {
    private DwdOrderDetail() {}

    public static SingleOutputStreamOperator<OrderDetail> build(
            DataStream<AdClickEvent> adClickStream,
            DataStream<OrderDetail> orderDetailStream) {
        return adClickStream
                .keyBy(AttributionKey::from)
                .connect(orderDetailStream.keyBy(AttributionKey::from))
                .process(new AttributionProcessFunction())
                .name("DwdOrderDetail: dwd_ad_action_log connect ods_order_info");
    }
}
