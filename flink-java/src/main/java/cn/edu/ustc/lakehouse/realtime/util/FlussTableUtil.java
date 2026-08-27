package cn.edu.ustc.lakehouse.realtime.util;

import cn.edu.ustc.lakehouse.realtime.config.RealtimeJobConfig;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/** Creates the shared Flink/Fluss execution context without hiding source or sink semantics. */
public final class FlussTableUtil {
    private FlussTableUtil() {}

    public static Context createContext(RealtimeJobConfig config) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(config.parallelism());
        env.enableCheckpointing(30_000L, CheckpointingMode.EXACTLY_ONCE);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
                env, EnvironmentSettings.newInstance().inStreamingMode().build());
        tableEnv.getConfig().set("table.local-time-zone", "Asia/Shanghai");
        tableEnv.getConfig().set("table.dynamic-table-options.enabled", "true");
        tableEnv.getConfig().set("table.exec.sink.upsert-materialize", "NONE");
        tableEnv.executeSql("CREATE CATALOG fluss WITH ('type'='fluss','bootstrap.servers'='"
                + sqlLiteral(config.flussBootstrapServers()) + "')");
        return new Context(env, tableEnv);
    }

    public static String scanHint(RealtimeJobConfig config) {
        String mode = "latest".equalsIgnoreCase(config.startupMode()) ? "latest" : "earliest";
        return " /*+ OPTIONS('scan.startup.mode'='" + mode + "') */ ";
    }

    private static String sqlLiteral(String value) {
        return value.replace("'", "''");
    }

    public record Context(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {}
}
