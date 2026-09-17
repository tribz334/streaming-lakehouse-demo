package cn.edu.ustc.lakehouse.realtime.util;

import cn.edu.ustc.lakehouse.realtime.config.RealtimeJobConfig;
import cn.edu.ustc.lakehouse.realtime.model.BaseDim;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.CompletableFuture;

/** Shared Flink/Fluss execution and table-access helpers. */
public final class FlussUtil {
    private FlussUtil() {}

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

    public static Table readTable(
            Context context, RealtimeJobConfig config, String tableName, String projection) {
        return context.tableEnv().sqlQuery("SELECT " + projection + " FROM fluss."
                + config.flussDatabase() + "." + tableName + scanHint(config));
    }

    /**
     * Builds a Table API lookup/enrichment query. The connector executes the lookup;
     * domain objects remain responsible only for mapping the returned Row.
     */
    public static Table lookup(Context context, String lookupSql) {
        return context.tableEnv().sqlQuery(lookupSql);
    }

    /** Opens one reusable Fluss primary-key lookup handle for an async operator instance. */
    public static LookupHandle openLookup(RealtimeJobConfig config, BaseDim<?> dimension) {
        Configuration flussConfig = new Configuration();
        flussConfig.setString("bootstrap.servers", config.flussBootstrapServers());
        Connection connection = ConnectionFactory.createConnection(flussConfig);
        org.apache.fluss.client.table.Table table = connection.getTable(
                TablePath.of(config.flussDatabase(), dimension.getTableName()));
        return new LookupHandle(connection, table, table.newLookup().createLookuper());
    }

    /** Performs a non-blocking primary-key lookup and returns the complete dimension row. */
    public static CompletableFuture<InternalRow> lookup(LookupHandle handle, Object key) {
        return handle.lookuper().lookup(GenericRow.of(toInternalKey(key)))
                .thenApply(result -> result.getSingletonRow());
    }

    public static String scanHint(RealtimeJobConfig config) {
        String mode = "latest".equalsIgnoreCase(config.startupMode()) ? "latest" : "earliest";
        return " /*+ OPTIONS('scan.startup.mode'='" + mode + "') */ ";
    }

    private static String sqlLiteral(String value) {
        return value.replace("'", "''");
    }

    private static Object toInternalKey(Object key) {
        if (key instanceof String string) {
            return BinaryString.fromString(string);
        }
        return key;
    }

    public record Context(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {}

    /** Per-operator Fluss resources; {@link org.apache.fluss.client.lookup.Lookuper} is not shared. */
    public record LookupHandle(
            Connection connection,
            org.apache.fluss.client.table.Table table,
            Lookuper lookuper) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            try {
                table.close();
            } finally {
                connection.close();
            }
        }
    }
}
