package com.nio.redis.framework.client.output;
import com.nio.redis.framework.client.codec.RedisCodec;
import com.nio.redis.framework.client.protocol.CommandOutput;

import java.nio.ByteBuffer;
/**
 * Boolean output. The actual value is returned as an integer
 * where 0 indicates false and 1 indicates true, or as a null
 * bulk reply for script output.
 *
 */
public class BooleanOutput<K, V> extends CommandOutput<K, V, Boolean> {
    public BooleanOutput(RedisCodec<K, V> codec) {
        super(codec, null);
    }

    @Override
    public void set(long integer) {
        output = (integer == 1) ? Boolean.TRUE : Boolean.FALSE;
    }

    @Override
    public void set(ByteBuffer bytes) {
        output = (bytes != null) ? Boolean.TRUE : Boolean.FALSE;
    }
}
