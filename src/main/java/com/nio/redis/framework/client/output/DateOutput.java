

package com.nio.redis.framework.client.output;

import com.nio.redis.framework.client.codec.RedisCodec;
import com.nio.redis.framework.client.protocol.CommandOutput;

import java.util.Date;

/**
 * Date output with no milliseconds.
 *
 */
public class DateOutput<K, V> extends CommandOutput<K, V, Date> {
    public DateOutput(RedisCodec<K, V> codec) {
        super(codec, null);
    }

    @Override
    public void set(long time) {
        output = new Date(time * 1000);
    }
}
