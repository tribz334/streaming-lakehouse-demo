package cn.edu.ustc.lakehouse.realtime.job;

import cn.edu.ustc.lakehouse.realtime.config.RealtimeJobConfig;
import cn.edu.ustc.lakehouse.realtime.dwd.DwdLogProcessFunction;
import cn.edu.ustc.lakehouse.realtime.model.DirtyLog;
import cn.edu.ustc.lakehouse.realtime.model.ParsedAdEvent;
import cn.edu.ustc.lakehouse.realtime.model.RawLog;
import cn.edu.ustc.lakehouse.realtime.util.FlussTableUtil;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.util.OutputTag;

/** Fluss ODS -> DataStream validation/dirty split -> Fluss DWD. */
public final class DwdLogDataStreamJob {
    private static final OutputTag<DirtyLog> DIRTY =
            new OutputTag<>("dirty-sdk-log", TypeInformation.of(DirtyLog.class));

    private DwdLogDataStreamJob() {}

    public static void main(String[] args) {
        RealtimeJobConfig config = RealtimeJobConfig.fromArgs(args);
        FlussTableUtil.Context context = FlussTableUtil.createContext(config);
        context.tableEnv().getConfig().set("pipeline.name", "fluss-ods-log-datastream-to-dwd");
        String source = "SELECT msg_id AS msgId,bus_id AS busId,app_id AS appId,"
                + "log_id AS logId,common,events,ts,dt FROM fluss."
                + config.flussDatabase() + ".ods_log_di" + FlussTableUtil.scanHint(config);
        Table rawTable = context.tableEnv().sqlQuery(source);

        SingleOutputStreamOperator<ParsedAdEvent> parsed = context.tableEnv()
                .toDataStream(rawTable, RawLog.class)
                .process(new DwdLogProcessFunction(DIRTY))
                .name("parse-validate-and-split-ods-log");
        context.tableEnv().createTemporaryView("parsed_ad_event", parsed);
        context.tableEnv().createTemporaryView("parse_dirty_log", parsed.getSideOutput(DIRTY));

        StatementSet sinks = context.tableEnv().createStatementSet();
        sinks.addInsertSql("INSERT INTO fluss." + config.flussDatabase() + ".dwd_ad_event_di "
                + "SELECT p.eventId,p.uid,p.deviceId,p.platform,p.appVc,p.browserVc,p.sdkVc,"
                + "cp.advertiser_id,u.campaign_id,c.unit_id,p.creativeId,p.productId,p.slotId,"
                + "p.eventType,u.placement_type,u.ad_type,p.ts,TO_TIMESTAMP_LTZ(p.ts,3),p.dt,"
                + "DATE_FORMAT(TO_TIMESTAMP_LTZ(p.ts,3),'HH') "
                + "FROM parsed_ad_event p "
                + "JOIN fluss." + config.flussDatabase() + ".dim_creative_df c ON p.creativeId=c.creative_id "
                + "JOIN fluss." + config.flussDatabase() + ".dim_unit_df u ON c.unit_id=u.unit_id "
                + "JOIN fluss." + config.flussDatabase() + ".dim_campaign_df cp ON u.campaign_id=cp.campaign_id "
                + "WHERE u.placement_type BETWEEN 1 AND 6 AND u.ad_type BETWEEN 1 AND 4");
        sinks.addInsertSql("INSERT INTO fluss." + config.flussDatabase() + ".dwd_ad_event_dirty_di "
                + "SELECT eventId,creativeId,errorReason,common,events,ts,dt FROM parse_dirty_log "
                + "UNION ALL "
                + "SELECT p.eventId,p.creativeId,CASE "
                + "WHEN c.creative_id IS NULL THEN 'CREATIVE_NOT_FOUND' "
                + "WHEN u.unit_id IS NULL THEN 'UNIT_NOT_FOUND' "
                + "WHEN cp.campaign_id IS NULL THEN 'CAMPAIGN_NOT_FOUND' "
                + "WHEN u.placement_type IS NULL THEN 'PLACEMENT_TYPE_MISSING' "
                + "WHEN u.placement_type NOT BETWEEN 1 AND 6 THEN 'PLACEMENT_TYPE_OUT_OF_RANGE' "
                + "WHEN u.ad_type IS NULL THEN 'AD_TYPE_MISSING' "
                + "ELSE 'AD_TYPE_OUT_OF_RANGE' END,CAST(NULL AS STRING),"
                + "CAST(NULL AS STRING),p.ts,p.dt "
                + "FROM parsed_ad_event p "
                + "LEFT JOIN fluss." + config.flussDatabase() + ".dim_creative_df c ON p.creativeId=c.creative_id "
                + "LEFT JOIN fluss." + config.flussDatabase() + ".dim_unit_df u ON c.unit_id=u.unit_id "
                + "LEFT JOIN fluss." + config.flussDatabase() + ".dim_campaign_df cp ON u.campaign_id=cp.campaign_id "
                + "WHERE c.creative_id IS NULL OR u.unit_id IS NULL OR cp.campaign_id IS NULL "
                + "OR u.placement_type IS NULL OR u.placement_type NOT BETWEEN 1 AND 6 "
                + "OR u.ad_type IS NULL OR u.ad_type NOT BETWEEN 1 AND 4");
        sinks.execute();
    }
}
