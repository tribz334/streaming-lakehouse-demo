package cn.edu.ustc.lakehouse.realtime;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Collections;
import java.util.Map;

public final class RedisUtil implements AutoCloseable {
    private static final TypeReference<Map<String, Object>> DIM_TYPE = new TypeReference<>() {};

    private final JedisPool jedisPool;
    private final int ttlSeconds;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public RedisUtil(RealtimeJobConfig config) {
        this.jedisPool = new JedisPool(config.getRedisHost(), config.getRedisPort());
        this.ttlSeconds = config.getRedisTtlSeconds();
    }

    public Jedis getJedis() {
        return jedisPool.getResource();
    }

    public Map<String, Object> get(String key) {
        try (Jedis jedis = getJedis()) {
            String value = jedis.get(key);
            return value == null ? Collections.emptyMap() : objectMapper.readValue(value, DIM_TYPE);
        } catch (Exception cacheError) {
            return Collections.emptyMap();
        }
    }

    public void put(String key, Map<String, Object> dim) {
        if (dim == null || dim.isEmpty()) {
            return;
        }
        try (Jedis jedis = getJedis()) {
            jedis.setex(key, ttlSeconds, objectMapper.writeValueAsString(dim));
        } catch (Exception ignored) {
            // Cache failure must not block the realtime stream.
        }
    }

    @Override
    public void close() {
        jedisPool.close();
    }
}
