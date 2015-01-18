

package com.nio.redis.framework.client.output;

import com.nio.redis.framework.client.codec.RedisCodec;
import com.nio.redis.framework.client.protocol.CommandOutput;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

/**
 * {@link Set} of value output.
 *
 * @param <V> Value type.
 *
 */
public class ValueSetOutput<K, V> extends CommandOutput<K, V, Set<V>> {
    public ValueSetOutput(RedisCodec<K, V> codec) {
        super(codec, new HashSet<V>());
    }

    @Override
    public void set(ByteBuffer bytes) {
        output.add(bytes == null ? null : codec.decodeValue(bytes));
    }
}
