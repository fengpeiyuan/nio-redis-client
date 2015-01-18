package com.nio.redis.framework.client.exception;

/**
 * Exception thrown when the thread executing a redis command is
 * interrupted.
 *
 */
@SuppressWarnings("serial")
public class RedisCommandInterruptedException extends RedisException {
    public RedisCommandInterruptedException(Throwable e) {
        super("Command interrupted", e);
    }
}
