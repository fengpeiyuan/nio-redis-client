

package com.nio.redis.framework.client.output;

import java.nio.ByteBuffer;

import com.nio.redis.framework.client.RedisKeyValue;
import com.nio.redis.framework.client.codec.RedisCodec;
import com.nio.redis.framework.client.protocol.CommandOutput;

/**
 * Key-value pair output.
 *
 * @param <K> Key type.
 * @param <V> Value type.
 *
 */
public class KeyValueOutput<K, V> extends CommandOutput<K, V, RedisKeyValue<K, V>> {
    private K key;

    public KeyValueOutput(RedisCodec<K, V> codec) {
        super(codec, null);
    }

    @Override
    public void set(ByteBuffer bytes) {
        if (bytes != null) {
            if (key == null) {
                key = codec.decodeKey(bytes);
            } else {
                V value = codec.decodeValue(bytes);
                output = new RedisKeyValue<K, V>(key, value);
            }
        }
    }
}
