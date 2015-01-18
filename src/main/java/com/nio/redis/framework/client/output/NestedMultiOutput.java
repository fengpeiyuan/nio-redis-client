

package com.nio.redis.framework.client.output;

import com.nio.redis.framework.client.codec.RedisCodec;
import com.nio.redis.framework.client.exception.RedisException;
import com.nio.redis.framework.client.protocol.CommandOutput;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * {@link List} of command outputs, possibly deeply nested.
 *
 */
public class NestedMultiOutput<K, V> extends CommandOutput<K, V, List<Object>> {
    private LinkedList<List<Object>> stack;
    private int depth;

    public NestedMultiOutput(RedisCodec<K, V> codec) {
        super(codec, new ArrayList<Object>());
        stack = new LinkedList<List<Object>>();
        depth = 1;
    }

    @Override
    public void set(long integer) {
        output.add(integer);
    }

    @Override
    public void set(ByteBuffer bytes) {
        output.add(bytes == null ? null : codec.decodeValue(bytes));
    }

    @Override
    public void setError(ByteBuffer error) {
        output.add(new RedisException(decodeAscii(error)));
    }

    @Override
    public void complete(int depth) {
        if (depth > this.depth) {
            Object o = output.remove(output.size() - 1);
            stack.push(output);
            output = new ArrayList<Object>();
            output.add(o);
        } else if (depth > 0 && depth < this.depth) {
            stack.peek().add(output);
            output = stack.pop();
        }
        this.depth = depth;
    }
}
