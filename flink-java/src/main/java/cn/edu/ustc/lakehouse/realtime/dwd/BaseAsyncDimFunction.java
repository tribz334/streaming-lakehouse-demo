package cn.edu.ustc.lakehouse.realtime.dwd;

import cn.edu.ustc.lakehouse.realtime.config.RealtimeJobConfig;
import cn.edu.ustc.lakehouse.realtime.model.BaseDim;
import cn.edu.ustc.lakehouse.realtime.util.FlussUtil;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;

/** Shared non-blocking Fluss DIM lookup lifecycle and result handling. */
public abstract class BaseAsyncDimFunction<T, K, D extends BaseDim<K>>
        extends RichAsyncFunction<T, T> {
    private final RealtimeJobConfig config;
    private transient FlussUtil.LookupHandle lookupHandle;

    protected BaseAsyncDimFunction(RealtimeJobConfig config) {
        this.config = config;
    }

    protected abstract K getKey(T input);

    protected abstract D newDimension();

    protected abstract void enrich(T input, D dimension);

    @Override
    public void open(OpenContext openContext) {
        lookupHandle = FlussUtil.openLookup(config, newDimension());
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) {
        K key = getKey(input);
        if (key == null) {
            resultFuture.complete(Collections.singleton(input));
            return;
        }

        FlussUtil.lookup(lookupHandle, key).whenComplete((row, error) -> {
            if (error != null) {
                resultFuture.completeExceptionally(error);
                return;
            }
            if (row != null) {
                D dimension = newDimension();
                dimension.fromRow(row);
                enrich(input, dimension);
            }
            resultFuture.complete(Collections.singleton(input));
        });
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) {
        resultFuture.completeExceptionally(new IllegalStateException(
                "DIM lookup timed out for table " + newDimension().getTableName()
                        + " and key " + getKey(input)));
    }

    @Override
    public void close() throws Exception {
        if (lookupHandle != null) {
            lookupHandle.close();
        }
    }
}
