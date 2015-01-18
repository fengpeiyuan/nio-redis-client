

package com.nio.redis.framework.client.output;

import com.nio.redis.framework.client.codec.RedisCodec;
import com.nio.redis.framework.client.protocol.CommandOutput;

import java.nio.ByteBuffer;

/**
 * 64-bit integer output, may be null.
 *
 */
public class IntegerOutput<K, V> extends CommandOutput<K, V, Long> {
    public IntegerOutput(RedisCodec<K, V> codec) {
        super(codec, null);
    }

    @Override
    public void set(long integer) {
        output = integer;
    }

    @Override
    public void set(ByteBuffer bytes) {
        output = null;
    }
}
