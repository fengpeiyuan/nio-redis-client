

package com.nio.redis.framework.client.pubsub;

import static com.nio.redis.framework.client.protocol.CommandType.PSUBSCRIBE;
import static com.nio.redis.framework.client.protocol.CommandType.PUNSUBSCRIBE;
import static com.nio.redis.framework.client.protocol.CommandType.SUBSCRIBE;
import static com.nio.redis.framework.client.protocol.CommandType.UNSUBSCRIBE;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;

import com.nio.redis.framework.client.RedisAsync;
import com.nio.redis.framework.client.codec.RedisCodec;
import com.nio.redis.framework.client.protocol.Command;
import com.nio.redis.framework.client.protocol.CommandArgs;

/**
 * An asynchronous thread-safe pub/sub connection to a redis server. After one or
 * more channels are subscribed to only pub/sub related commands or {@link #quit}
 * may be called.
 *
 * Incoming messages and results of the {@link #subscribe}/{@link #unsubscribe}
 * calls will be passed to all registered {@link RedisPubSubListener}s.
 *
 * A {@link com.nio.redis.framework.client.protocol.ConnectionWatchdog} monitors each
 * connection and reconnects automatically until {@link #close} is called. Channel
 * and pattern subscriptions are renewed after reconnecting.
 *
 */
public class RedisPubSubConnection<K, V> extends RedisAsync<K, V> {
    private List<RedisPubSubListener<K, V>> listeners;
    private Set<K> channels;
    private Set<K> patterns;

    /**
     * Initialize a new connection.
     *
     * @param queue     Command queue.
     * @param codec     Codec used to encode/decode keys and values.
     * @param timeout   Maximum time to wait for a responses.
     * @param unit      Unit of time for the timeout.
     */
    public RedisPubSubConnection(BlockingQueue<Command<K, V, ?>> queue, RedisCodec<K, V> codec, long timeout, TimeUnit unit) {
        super(queue, codec, timeout, unit);
        listeners = new CopyOnWriteArrayList<RedisPubSubListener<K, V>>();
        channels  = new HashSet<K>();
        patterns  = new HashSet<K>();
    }

    /**
     * Add a new listener.
     *
     * @param listener Listener.
     */
    public void addListener(RedisPubSubListener<K, V> listener) {
        listeners.add(listener);
    }

    /**
     * Remove an existing listener.
     *
     * @param listener Listener.
     */
    public void removeListener(RedisPubSubListener<K, V> listener) {
        listeners.remove(listener);
    }

    public void psubscribe(K... patterns) {
        dispatch(PSUBSCRIBE, new PubSubOutput<K, V>(codec), args(patterns));
    }

    public void punsubscribe(K... patterns) {
        dispatch(PUNSUBSCRIBE, new PubSubOutput<K, V>(codec), args(patterns));
    }

    public void subscribe(K... channels) {
        dispatch(SUBSCRIBE, new PubSubOutput<K, V>(codec), args(channels));
    }

    public void unsubscribe(K... channels) {
        dispatch(UNSUBSCRIBE, new PubSubOutput<K, V>(codec), args(channels));
    }

    @Override
    public synchronized void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        super.channelConnected(ctx, e);

        if (channels.size() > 0) {
            subscribe(toArray(channels));
            channels.clear();
        }

        if (patterns.size() > 0) {
            psubscribe(toArray(patterns));
            patterns.clear();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        PubSubOutput<K, V> output = (PubSubOutput<K, V>) e.getMessage();
        for (RedisPubSubListener<K, V> listener : listeners) {
        	if(output.type().toString().equals("message")){
        	   listener.message(output.channel(), output.get());
               break;
        	}else if(output.type().toString().equals("pmessage")){
              listener.message(output.pattern(), output.channel(), output.get());
              break;
        	}else if(output.type().toString().equals("psubscribe")){
              patterns.add(output.pattern());
              listener.psubscribed(output.pattern(), output.count());
              break;
        	}else if(output.type().toString().equals("punsubscribe")){
              patterns.remove(output.pattern());
              listener.punsubscribed(output.pattern(), output.count());
              break;	
        	}else if(output.type().toString().equals("subscribe")){
              channels.add(output.channel());
              listener.subscribed(output.channel(), output.count());
              break;
        	}else if(output.type().toString().equals("unsubscribe")){
              channels.remove(output.channel());
              listener.unsubscribed(output.channel(), output.count());
              break;
        	}
        }
    }

    private CommandArgs<K, V> args(K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKeys(keys);
        return args;
    }

    @SuppressWarnings("unchecked")
    private <T> T[] toArray(Collection<T> c) {
        Class<T> cls = (Class<T>) c.iterator().next().getClass();
        T[] array = (T[]) Array.newInstance(cls, c.size());
        return c.toArray(array);
    }
}
