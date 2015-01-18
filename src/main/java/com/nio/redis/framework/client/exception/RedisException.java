package com.nio.redis.framework.client.exception;

/**
 * Exception thrown when redis returns an error message, or when the client
 * fails for any reason.
 *
 */
@SuppressWarnings("serial")
public class RedisException extends RuntimeException {
    public RedisException(String msg) {
        super(msg);
    }

    public RedisException(String msg, Throwable e) {
        super(msg, e);
    }
}
