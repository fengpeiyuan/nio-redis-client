

package com.nio.redis.framework.client.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * {@link Charset}-related utilities.
 *
 */
public class Charsets {
    public static final Charset ASCII = Charset.forName("US-ASCII");

    public static ByteBuffer buffer(String s) {
        return ByteBuffer.wrap(s.getBytes(ASCII));
    }
}
