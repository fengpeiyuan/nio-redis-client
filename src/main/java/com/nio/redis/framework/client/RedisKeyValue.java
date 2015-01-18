package com.nio.redis.framework.client;

/**
 * A key-value pair.
 *
 */
public class RedisKeyValue<K, V> {
    public final K key;
    public final V value;

    public RedisKeyValue(K key, V value) {
        this.key   = key;
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        RedisKeyValue<?, ?> that = (RedisKeyValue<?, ?>) o;
        return key.equals(that.key) && value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return 31 * key.hashCode() + value.hashCode();
    }

    @Override
    public String toString() {
        return String.format("(%s, %s)", key, value);
    }
}
