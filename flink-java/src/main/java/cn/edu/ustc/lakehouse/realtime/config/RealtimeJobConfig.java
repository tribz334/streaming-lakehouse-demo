package cn.edu.ustc.lakehouse.realtime.config;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/** Runtime options shared by the Fluss-backed DataStream jobs. */
public final class RealtimeJobConfig implements Serializable {
    private final String flussBootstrapServers;
    private final String flussDatabase;
    private final String startupMode;
    private final int parallelism;
    private final Duration outOfOrderness;
    private final Duration sourceIdleness;
    private final Duration attributionAllowedLateness;
    private final int realtimeMetricWindowSeconds;

    private RealtimeJobConfig(Map<String, String> values) {
        flussBootstrapServers = values.getOrDefault("fluss-bootstrap", "fluss-coordinator:9123");
        flussDatabase = values.getOrDefault("fluss-database", "ad_dw");
        startupMode = values.getOrDefault("startup-mode", "earliest");
        parallelism = Integer.parseInt(values.getOrDefault("parallelism", "1"));
        outOfOrderness = Duration.ofSeconds(Long.parseLong(values.getOrDefault("out-of-orderness-seconds", "10")));
        sourceIdleness = Duration.ofSeconds(Long.parseLong(values.getOrDefault("source-idleness-seconds", "30")));
        attributionAllowedLateness = Duration.ofSeconds(Long.parseLong(
                values.getOrDefault("attribution-allowed-lateness-seconds", "10")));
        realtimeMetricWindowSeconds = Integer.parseInt(
                values.getOrDefault("realtime-metric-window-seconds", "10"));
        if (realtimeMetricWindowSeconds <= 0) {
            throw new IllegalArgumentException("realtime-metric-window-seconds must be greater than zero");
        }
        if (attributionAllowedLateness.isNegative()) {
            throw new IllegalArgumentException("attribution-allowed-lateness-seconds cannot be negative");
        }
    }

    public static RealtimeJobConfig fromArgs(String[] args) {
        Map<String, String> values = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--") && i + 1 < args.length) {
                values.put(args[i].substring(2), args[++i]);
            }
        }
        return new RealtimeJobConfig(values);
    }

    public String flussBootstrapServers() { return flussBootstrapServers; }
    public String flussDatabase() { return flussDatabase; }
    public String startupMode() { return startupMode; }
    public int parallelism() { return parallelism; }
    public Duration outOfOrderness() { return outOfOrderness; }
    public Duration sourceIdleness() { return sourceIdleness; }
    public Duration attributionAllowedLateness() { return attributionAllowedLateness; }
    public int realtimeMetricWindowSeconds() { return realtimeMetricWindowSeconds; }
}
