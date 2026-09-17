package cn.edu.ustc.lakehouse.realtime.job;

import cn.edu.ustc.lakehouse.realtime.config.RealtimeJobConfig;
import cn.edu.ustc.lakehouse.realtime.dwd.DwdLogProcessFunction;
import cn.edu.ustc.lakehouse.realtime.model.AdEvent;
import cn.edu.ustc.lakehouse.realtime.model.DirtyLog;
import cn.edu.ustc.lakehouse.realtime.model.RawLog;
import cn.edu.ustc.lakehouse.realtime.util.FlussUtil;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.util.OutputTag;

/** Fluss ODS -> DataStream validation/dirty split -> Fluss DWD. */
public final class DwdLogDataStreamJob {
    private static final OutputTag<DirtyLog> DIRTY =
            new OutputTag<>("dirty-sdk-log", TypeInformation.of(DirtyLog.class));

    private DwdLogDataStreamJob() {}

    public static void main(String[] args) throws Exception {
        RealtimeJobConfig config = RealtimeJobConfig.fromArgs(args);
        FlussUtil.Context context = FlussUtil.createContext(config);
        context.tableEnv().getConfig().set("pipeline.name", "fluss-ods-log-datastream-to-dwd");

        DataStream<RawLog> rawLogs = createSource(context, config);
        SingleOutputStreamOperator<AdEvent> adEvents = rawLogs
                .process(new DwdLogProcessFunction(DIRTY))
                .name("parse-validate-and-split-ods-log");
        DataStream<DirtyLog> dirtyLogs = adEvents.getSideOutput(DIRTY);

        StatementSet sinks = context.tableEnv().createStatementSet();
        sinkAdEvent(adEvents, context, config, sinks);
        sinkDirtyLog(dirtyLogs, context, config, sinks);
        sinks.execute();
    }

    private static DataStream<RawLog> createSource(
            FlussUtil.Context context, RealtimeJobConfig config) {
        String source = "SELECT msg_id AS msgId,bus_id AS busId,app_id AS appId,"
                + "log_id AS logId,common,events,ts,dt FROM fluss."
                + config.flussDatabase() + ".ods_log" + FlussUtil.scanHint(config);
        return context.tableEnv().toDataStream(context.tableEnv().sqlQuery(source), RawLog.class);
    }

    private static void sinkAdEvent(
            DataStream<AdEvent> stream,
            FlussUtil.Context context,
            RealtimeJobConfig config,
            StatementSet sinks) {
        context.tableEnv().createTemporaryView("ad_event", stream);
        sinks.addInsertSql("INSERT INTO fluss." + config.flussDatabase() + ".dwd_ad_event_di "
                + "SELECT p.eventId,p.uid,p.deviceId,p.platform,p.appVc,p.browserVc,p.sdkVc,"
                + "p.eventType,p.creativeId,p.productId,p.slotId,p.ts,"
                + "DATE_FORMAT(TO_TIMESTAMP_LTZ(p.ts,3),'yyyyMMdd'),"
                + "DATE_FORMAT(TO_TIMESTAMP_LTZ(p.ts,3),'HH') "
                + "FROM ad_event p");
    }

    private static void sinkDirtyLog(
            DataStream<DirtyLog> stream,
            FlussUtil.Context context,
            RealtimeJobConfig config,
            StatementSet sinks) {
        context.tableEnv().createTemporaryView("dirty_log", stream);
        sinks.addInsertSql("INSERT INTO fluss." + config.flussDatabase() + ".ods_dirty_log "
                + "SELECT rawData,errorReason,errorTime,dt FROM dirty_log");
    }
}
