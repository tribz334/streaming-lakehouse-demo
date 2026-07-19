package cn.edu.ustc.lakehouse.realtime;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public final class RealtimeJobConfig implements Serializable {
    private final String kafkaBootstrapServers;
    private final String sourceTopic;
    private final String consumerGroup;
    private final String startupMode;
    private final String dimensionJdbcUrl;
    private final String dimensionUsername;
    private final String dimensionPassword;
    private final String redisHost;
    private final int redisPort;
    private final int redisTtlSeconds;
    private final String starRocksJdbcUrl;
    private final String starRocksTable;
    private final String starRocksUsername;
    private final String starRocksPassword;
    private final int parallelism;

    private RealtimeJobConfig(
            String kafkaBootstrapServers,
            String sourceTopic,
            String consumerGroup,
            String startupMode,
            String dimensionJdbcUrl,
            String dimensionUsername,
            String dimensionPassword,
            String redisHost,
            int redisPort,
            int redisTtlSeconds,
            String starRocksJdbcUrl,
            String starRocksTable,
            String starRocksUsername,
            String starRocksPassword,
            int parallelism) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.sourceTopic = sourceTopic;
        this.consumerGroup = consumerGroup;
        this.startupMode = startupMode;
        this.dimensionJdbcUrl = dimensionJdbcUrl;
        this.dimensionUsername = dimensionUsername;
        this.dimensionPassword = dimensionPassword;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisTtlSeconds = redisTtlSeconds;
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
                parameters.getOrDefault("consumer-group", "flink-java-realtime-metric"),
                parameters.getOrDefault("startup-mode", "earliest"),
                parameters.getOrDefault("dimension-jdbc-url", "jdbc:mysql://mysql:3306/ad_ods"),
                parameters.getOrDefault("dimension-username", "root"),
                parameters.getOrDefault("dimension-password", "root"),
                parameters.getOrDefault("redis-host", "redis"),
                Integer.parseInt(parameters.getOrDefault("redis-port", "6379")),
                Integer.parseInt(parameters.getOrDefault("redis-ttl-seconds", "3600")),
                parameters.getOrDefault("starrocks-jdbc-url", "jdbc:mysql://starrocks:9030/ad_ads"),
                parameters.getOrDefault("starrocks-table", "realtime_ad_metrics_10s"),
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
    public String getConsumerGroup() { return consumerGroup; }
    public String getStartupMode() { return startupMode; }
    public String getDimensionJdbcUrl() { return dimensionJdbcUrl; }
    public String getDimensionUsername() { return dimensionUsername; }
    public String getDimensionPassword() { return dimensionPassword; }
    public String getRedisHost() { return redisHost; }
    public int getRedisPort() { return redisPort; }
    public int getRedisTtlSeconds() { return redisTtlSeconds; }
    public String getStarRocksJdbcUrl() { return starRocksJdbcUrl; }
    public String getStarRocksTable() { return starRocksTable; }
    public String getStarRocksUsername() { return starRocksUsername; }
    public String getStarRocksPassword() { return starRocksPassword; }
    public int getParallelism() { return parallelism; }
}
