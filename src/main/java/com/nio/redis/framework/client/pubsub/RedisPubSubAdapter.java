

package com.nio.redis.framework.client.pubsub;

/**
 * Convenience adapter with an empty implementation of all
 * {@link RedisPubSubListener} callback methods.
 *
 * @param <V> Value type.
 *
 */
public class RedisPubSubAdapter<K, V> implements RedisPubSubListener<K, V> {
    
    public void message(K channel, V message) {
    }

    
    public void message(K pattern, K channel, V message) {
    }

    
    public void subscribed(K channel, long count) {
    }

    
    public void psubscribed(K pattern, long count) {
    }

    
    public void unsubscribed(K channel, long count) {
    }

    
    public void punsubscribed(K pattern, long count) {
    }
}
