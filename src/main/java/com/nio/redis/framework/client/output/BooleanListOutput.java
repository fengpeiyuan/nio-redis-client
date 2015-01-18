package com.nio.redis.framework.client.output;

import java.util.ArrayList;
import java.util.List;

import com.nio.redis.framework.client.codec.RedisCodec;
import com.nio.redis.framework.client.protocol.CommandOutput;

/**
 * {@link java.util.List} of boolean output.
 *
 */
public class BooleanListOutput<K, V> extends CommandOutput<K, V, List<Boolean>> {
    public BooleanListOutput(RedisCodec<K, V> codec) {
        super(codec, new ArrayList<Boolean>());
    }

    @Override
    public void set(long integer) {
        output.add((integer == 1) ? Boolean.TRUE : Boolean.FALSE);
    }
}
