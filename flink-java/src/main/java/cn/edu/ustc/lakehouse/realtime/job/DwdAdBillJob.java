package cn.edu.ustc.lakehouse.realtime.job;

import cn.edu.ustc.lakehouse.realtime.config.FlussTableNames;
import cn.edu.ustc.lakehouse.realtime.config.RealtimeJobConfig;
import cn.edu.ustc.lakehouse.realtime.dwd.AdBillDimAsyncFunction;
import cn.edu.ustc.lakehouse.realtime.model.AdBill;
import cn.edu.ustc.lakehouse.realtime.util.FlussUtil;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.concurrent.TimeUnit;

/** ODS bill facts -> asynchronous creative DIM lookup -> dwd_ad_bill_di. */
public final class DwdAdBillJob {
    private DwdAdBillJob() {}

    public static void main(String[] args) {
        RealtimeJobConfig config = RealtimeJobConfig.fromArgs(args);
        FlussUtil.Context context = FlussUtil.createContext(config);
        context.tableEnv().getConfig().set("pipeline.name", "fluss-dwd-ad-bill-enrichment");

        Table sourceTable = FlussUtil.readTable(
                context,
                config,
                FlussTableNames.ODS_BILL_INFO,
                "bill_id,advertiser_id,campaign_id,unit_id,creative_id,user_id,slot_id,"
                        + "billing_type,commerce_channel,cost,bill_time,updated_at,ts,dt,`hour`");

        DataStream<AdBill> bills = context.tableEnv().toChangelogStream(sourceTable)
                .filter(DwdAdBillJob::isPositiveChange)
                .map(DwdAdBillJob::toAdBill)
                .returns(AdBill.class)
                .name("fluss-ods-bill-source");

        DataStream<AdBill> enrichedBills = AsyncDataStream.unorderedWait(
                        bills,
                        new AdBillDimAsyncFunction(config),
                        config.dimLookupTimeout().toMillis(),
                        TimeUnit.MILLISECONDS,
                        config.dimLookupCapacity())
                .name("creative-dim-async-lookup");

        context.tableEnv().createTemporaryView("dwd_ad_bill", enrichedBills);
        context.tableEnv().executeSql("INSERT INTO fluss." + config.flussDatabase() + "."
                + FlussTableNames.DWD_AD_BILL + " "
                + "SELECT billId,creativeId,unitId,campaignId,advertiserId,slotId,uid,"
                + "billingType,commerceChannel,cost,isClosed,ts,updatedAt,dt,`hour` "
                + "FROM dwd_ad_bill");
    }

    private static boolean isPositiveChange(Row row) {
        return row.getKind() == RowKind.INSERT || row.getKind() == RowKind.UPDATE_AFTER;
    }

    private static AdBill toAdBill(Row row) {
        AdBill bill = new AdBill();
        bill.billId = requiredLong(row, 0);
        bill.advertiserId = nullableLong(row, 1);
        bill.campaignId = nullableLong(row, 2);
        bill.unitId = nullableLong(row, 3);
        bill.creativeId = requiredLong(row, 4);
        bill.uid = nullableLong(row, 5);
        bill.slotId = nullableLong(row, 6);
        bill.billingType = requiredInt(row, 7);
        bill.commerceChannel = string(row, 8);
        bill.cost = requiredLong(row, 9);
        bill.updatedAt = string(row, 11);
        bill.ts = requiredLong(row, 12);
        bill.dt = string(row, 13);
        bill.hour = string(row, 14);
        return bill;
    }

    private static long requiredLong(Row row, int position) {
        Object value = row.getField(position);
        if (value instanceof Number number) return number.longValue();
        throw new IllegalArgumentException("Expected numeric field at position " + position);
    }

    private static int requiredInt(Row row, int position) {
        return Math.toIntExact(requiredLong(row, position));
    }

    private static Long nullableLong(Row row, int position) {
        Object value = row.getField(position);
        return value instanceof Number number ? number.longValue() : null;
    }

    private static String string(Row row, int position) {
        Object value = row.getField(position);
        return value == null ? null : value.toString();
    }
}
