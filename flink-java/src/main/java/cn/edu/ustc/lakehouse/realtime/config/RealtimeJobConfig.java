package cn.edu.ustc.lakehouse.realtime.config;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public final class RealtimeJobConfig implements Serializable {
    private final String kafkaBootstrapServers;
    private final String sourceTopic;
    private final String dwdAdActionTopic;
    private final String dwdPageLogTopic;
    private final String dwdDirtyLogTopic;
    private final String dwdOrderDetailTopic;
    private final String consumerGroup;
    private final String startupMode;
    private final String orderMysqlHostname;
    private final int orderMysqlPort;
    private final String orderMysqlDatabase;
    private final String orderMysqlTable;
    private final String orderMysqlUsername;
    private final String orderMysqlPassword;
    private final String orderMysqlServerId;
    private final String orderMysqlStartupMode;
    private final String orderTimeZone;
    private final String starRocksJdbcUrl;
    private final String starRocksTable;
    private final String starRocksUsername;
    private final String starRocksPassword;
    private final int parallelism;

    private RealtimeJobConfig(
            String kafkaBootstrapServers,
            String sourceTopic,
            String dwdAdActionTopic,
            String dwdPageLogTopic,
            String dwdDirtyLogTopic,
            String dwdOrderDetailTopic,
            String consumerGroup,
            String startupMode,
            String orderMysqlHostname,
            int orderMysqlPort,
            String orderMysqlDatabase,
            String orderMysqlTable,
            String orderMysqlUsername,
            String orderMysqlPassword,
            String orderMysqlServerId,
            String orderMysqlStartupMode,
            String orderTimeZone,
            String starRocksJdbcUrl,
            String starRocksTable,
            String starRocksUsername,
            String starRocksPassword,
            int parallelism) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.sourceTopic = sourceTopic;
        this.dwdAdActionTopic = dwdAdActionTopic;
        this.dwdPageLogTopic = dwdPageLogTopic;
        this.dwdDirtyLogTopic = dwdDirtyLogTopic;
        this.dwdOrderDetailTopic = dwdOrderDetailTopic;
        this.consumerGroup = consumerGroup;
        this.startupMode = startupMode;
        this.orderMysqlHostname = orderMysqlHostname;
        this.orderMysqlPort = orderMysqlPort;
        this.orderMysqlDatabase = orderMysqlDatabase;
        this.orderMysqlTable = orderMysqlTable;
        this.orderMysqlUsername = orderMysqlUsername;
        this.orderMysqlPassword = orderMysqlPassword;
        this.orderMysqlServerId = orderMysqlServerId;
        this.orderMysqlStartupMode = orderMysqlStartupMode;
        this.orderTimeZone = orderTimeZone;
        this.starRocksJdbcUrl = starRocksJdbcUrl;
        this.starRocksTable = starRocksTable;
        this.starRocksUsername = starRocksUsername;
        this.starRocksPassword = starRocksPassword;
        this.parallelism = parallelism;
    }

    public static RealtimeJobConfig fromArgs(String[] args) {
        Map<String, String> parameters = parseArgs(args);
        return new RealtimeJobConfig(
                parameters.getOrDefault("kafka-bootstrap", "kafka-node-1:9092"),
                parameters.getOrDefault("source-topic", "ods_log"),
                parameters.getOrDefault("dwd-ad-action-topic", "dwd_ad_action_log"),
                parameters.getOrDefault("dwd-page-log-topic", "dwd_page_log"),
                parameters.getOrDefault("dwd-dirty-log-topic", "dwd_dirty_log"),
                parameters.getOrDefault("dwd-order-detail-topic", "dwd_order_detail"),
                parameters.getOrDefault("consumer-group", "flink-java-realtime-metric"),
                parameters.getOrDefault("startup-mode", "earliest"),
                parameters.getOrDefault("order-mysql-hostname", "mysql"),
                Integer.parseInt(parameters.getOrDefault("order-mysql-port", "3306")),
                parameters.getOrDefault("order-mysql-database", "ad_ods"),
                parameters.getOrDefault("order-mysql-table", "ad_order"),
                parameters.getOrDefault("order-mysql-username", "root"),
                parameters.getOrDefault("order-mysql-password", "root"),
                parameters.getOrDefault("order-mysql-server-id", "5501-5508"),
                parameters.getOrDefault("order-mysql-startup-mode", "latest-offset"),
                parameters.getOrDefault("order-time-zone", "Asia/Shanghai"),
                parameters.getOrDefault("starrocks-jdbc-url", "jdbc:mysql://starrocks:9030/ad_ads"),
                parameters.getOrDefault("starrocks-table", "realtime_ad_attribution_metrics_10s"),
                parameters.getOrDefault("starrocks-username", "root"),
                parameters.getOrDefault("starrocks-password", ""),
                Integer.parseInt(parameters.getOrDefault("parallelism", "1")));
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> parameters = new HashMap<>();
        for (int index = 0; index < args.length; index++) {
            String argument = args[index];
            if (!argument.startsWith("--") || index + 1 >= args.length) {
                continue;
            }
            parameters.put(argument.substring(2), args[++index]);
        }
        return parameters;
    }

    public String getKafkaBootstrapServers() { return kafkaBootstrapServers; }
    public String getSourceTopic() { return sourceTopic; }
    public String getDwdAdActionTopic() { return dwdAdActionTopic; }
    public String getDwdPageLogTopic() { return dwdPageLogTopic; }
    public String getDwdDirtyLogTopic() { return dwdDirtyLogTopic; }
    public String getDwdOrderDetailTopic() { return dwdOrderDetailTopic; }
    public String getConsumerGroup() { return consumerGroup; }
    public String getStartupMode() { return startupMode; }
    public String getOrderMysqlHostname() { return orderMysqlHostname; }
    public int getOrderMysqlPort() { return orderMysqlPort; }
    public String getOrderMysqlDatabase() { return orderMysqlDatabase; }
    public String getOrderMysqlTable() { return orderMysqlTable; }
    public String getOrderMysqlUsername() { return orderMysqlUsername; }
    public String getOrderMysqlPassword() { return orderMysqlPassword; }
    public String getOrderMysqlServerId() { return orderMysqlServerId; }
    public String getOrderMysqlStartupMode() { return orderMysqlStartupMode; }
    public String getOrderTimeZone() { return orderTimeZone; }
    public String getStarRocksJdbcUrl() { return starRocksJdbcUrl; }
    public String getStarRocksTable() { return starRocksTable; }
    public String getStarRocksUsername() { return starRocksUsername; }
    public String getStarRocksPassword() { return starRocksPassword; }
    public int getParallelism() { return parallelism; }
}
