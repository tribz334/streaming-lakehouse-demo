package cn.edu.ustc.lakehouse.realtime;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Reusable asynchronous dimension lookup template.
 *
 * <p>Concrete dim functions only provide the lookup key, table name, and join
 * rule. Redis cache access, MySQL fallback, threading, and timeout handling
 * remain shared.</p>
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> {
    private final RealtimeJobConfig config;
    private transient ExecutorService executor;
    private transient RedisUtil redisUtil;
    private transient MySQLUtil mysqlUtil;

    protected DimAsyncFunction(RealtimeJobConfig config) {
        this.config = config;
    }

    @Override
    public void open(OpenContext openContext) {
        executor = Executors.newFixedThreadPool(4);
        redisUtil = new RedisUtil(config);
        mysqlUtil = new MySQLUtil(config);
    }

    @Override
    public void asyncInvoke(T data, ResultFuture<T> resultFuture) {
        executor.submit(() -> {
            try {
                String key = getKey(data);
                Map<String, Object> dim = lookupDim(key);
                join(data, dim);
            } catch (Exception lookupError) {
                join(data, Collections.emptyMap());
            }
            resultFuture.complete(Collections.singleton(data));
        });
    }

    @Override
    public void timeout(T data, ResultFuture<T> resultFuture) {
        join(data, Collections.emptyMap());
        resultFuture.complete(Collections.singleton(data));
    }

    @Override
    public void close() {
        if (executor != null) {
            executor.shutdownNow();
        }
        if (redisUtil != null) {
            redisUtil.close();
        }
    }

    protected abstract String getKey(T data);

    protected abstract String getTableName();

    protected abstract void join(T data, Map<String, Object> dim);

    private Map<String, Object> lookupDim(String key) {
        if (key == null || key.isBlank()) {
            return Collections.emptyMap();
        }
        String cacheKey = "dim:" + getTableName() + ":" + key;
        Map<String, Object> dim = redisUtil.get(cacheKey);
        if (!dim.isEmpty()) {
            return dim;
        }
        dim = mysqlUtil.findByKey(getTableName(), key);
        redisUtil.put(cacheKey, dim);
        return dim;
    }
}
