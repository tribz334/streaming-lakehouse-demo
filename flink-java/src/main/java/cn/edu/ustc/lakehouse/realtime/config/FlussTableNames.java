package cn.edu.ustc.lakehouse.realtime.config;

/** Canonical Fluss table names shared by the realtime jobs. */
public final class FlussTableNames {
    public static final String ODS_BILL_INFO = "ods_mysql_bill_info";
    public static final String ODS_ORDER_INFO = "ods_mysql_order_info";

    public static final String DIM_CREATIVE = "dim_creative_df";
    public static final String DIM_UNIT = "dim_unit_df";
    public static final String DIM_CAMPAIGN = "dim_campaign_df";
    public static final String DIM_ADVERTISER = "dim_advertiser_df";

    public static final String DWD_AD_EVENT = "dwd_ad_event_di";
    public static final String DWD_AD_BILL = "dwd_ad_bill_di";
    public static final String DWD_ORDER = "dwd_order_acc";

    private FlussTableNames() {}
}
