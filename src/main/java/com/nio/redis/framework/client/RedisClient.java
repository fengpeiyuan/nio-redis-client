package com.nio.redis.framework.client;


import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;

import com.nio.redis.framework.client.codec.RedisCodec;
import com.nio.redis.framework.client.codec.Utf8StringCodec;
import com.nio.redis.framework.client.exception.RedisException;
import com.nio.redis.framework.client.protocol.Command;
import com.nio.redis.framework.client.protocol.CommandHandler;
import com.nio.redis.framework.client.protocol.ConnectionWatchdog;
import com.nio.redis.framework.client.pubsub.PubSubCommandHandler;
import com.nio.redis.framework.client.pubsub.RedisPubSubConnection;

/**
 * A scalable thread-safe <a href="http://redis.io/">Redis</a> client. Multiple threads
 * may share one connection provided they avoid blocking and transactional operations
 * such as BLPOP and MULTI/EXEC.
 *
 */
public class RedisClient {
    private ClientBootstrap clientBootstrap;
    private Timer timer;
    private ChannelGroup channelGroup;
    private long timeout;
    private TimeUnit unit;
    private static HashedWheelTimer hashedWheelTimer = new HashedWheelTimer();


    /**
     * Create a new client that connects to the supplied host and port. Connection
     * attempts and non-blocking commands will {@link #setDefaultTimeout timeout}
     * after 60 seconds.
     *
     * @param host    Server hostname.
     * @param port    Server port.
     */
    public RedisClient(String host, int port) {
        ExecutorService boss = Executors.newFixedThreadPool(1);
        ExecutorService workers    = Executors.newCachedThreadPool();
        ClientSocketChannelFactory factory = new NioClientSocketChannelFactory(boss, workers);
        InetSocketAddress addr = new InetSocketAddress(host, port);
        clientBootstrap = new ClientBootstrap(factory);
        clientBootstrap.setOption("remoteAddress", addr);
        setDefaultTimeout(60, TimeUnit.SECONDS);
        channelGroup = new DefaultChannelGroup();
        timer    = hashedWheelTimer;
    }
    
    /**
     *  Create a new client that connects to the supplied host and port. Connection
     * attempts and non-blocking commands will {@link #setDefaultTimeout timeout}
     * after 60 seconds.
     * @param host
     * @param port
     * @param passwd
     */
    public RedisClient(String host, int port,String passwd) {
        ExecutorService boss = Executors.newFixedThreadPool(1);
        ExecutorService workers    = Executors.newCachedThreadPool();
        ClientSocketChannelFactory factory = new NioClientSocketChannelFactory(boss, workers);
        InetSocketAddress addr = new InetSocketAddress(host, port);
        clientBootstrap = new ClientBootstrap(factory);
        clientBootstrap.setOption("remoteAddress", addr);
        clientBootstrap.setOption("passwd", passwd);
        setDefaultTimeout(60, TimeUnit.SECONDS);
        channelGroup = new DefaultChannelGroup();
        timer    = hashedWheelTimer;
    }

    /**
     * Set the default timeout for {@link RedisConnection connections} created by
     * this client. The timeout applies to connection attempts and non-blocking
     * commands.
     *
     * @param timeout   Default connection timeout.
     * @param unit      Unit of time for the timeout.
     */
    public void setDefaultTimeout(long timeout, TimeUnit unit) {
        this.timeout = timeout;
        this.unit    = unit;
        clientBootstrap.setOption("connectTimeoutMillis", unit.toMillis(timeout));
    }

    /**
     * Open a new synchronous connection to the redis server that treats
     * keys and values as UTF-8 strings.
     *
     * @return A new connection.
     */
    public Redis<String, String> connect() {
        return connect(new Utf8StringCodec());
    }

    /**
     * Open a new asynchronous connection to the redis server that treats
     * keys and values as UTF-8 strings.
     *
     * @return A new connection.
     */
    public RedisAsync<String, String> connectAsync() {
        return connectAsync(new Utf8StringCodec());
    }

    /**
     * Open a new pub/sub connection to the redis server that treats
     * keys and values as UTF-8 strings.
     *
     * @return A new connection.
     */
    public RedisPubSubConnection<String, String> connectPubSub() {
        return connectPubSub(new Utf8StringCodec());
    }

    /**
     * Open a new synchronous connection to the redis server. Use the supplied
     * {@link RedisCodec codec} to encode/decode keys and values.
     *
     * @param codec Use this codec to encode/decode keys and values.
     *
     * @return A new connection.
     */
    public <K, V> Redis<K, V> connect(RedisCodec<K, V> codec) {
        return new Redis<K, V>(connectAsync(codec));
    }

    /**
     * Open a new asynchronous connection to the redis server. Use the supplied
     * {@link RedisCodec codec} to encode/decode keys and values.
     *
     * @param codec Use this codec to encode/decode keys and values.
     *
     * @return A new connection.
     */
    public <K, V> RedisAsync<K, V> connectAsync(RedisCodec<K, V> codec) {
        BlockingQueue<Command<K, V, ?>> queue = new LinkedBlockingQueue<Command<K, V, ?>>();

        CommandHandler<K, V> handler = new CommandHandler<K, V>(queue);
        RedisAsync<K, V> redisAsync = new RedisAsync<K, V>(queue, codec, timeout, unit);

        return connect(handler, redisAsync);
    }

    /**
     * Open a new pub/sub connection to the redis server. Use the supplied
     * {@link RedisCodec codec} to encode/decode keys and values.
     *
     * @param codec Use this codec to encode/decode keys and values.
     *
     * @return A new pub/sub connection.
     */
    public <K, V> RedisPubSubConnection<K, V> connectPubSub(RedisCodec<K, V> codec) {
        BlockingQueue<Command<K, V, ?>> queue = new LinkedBlockingQueue<Command<K, V, ?>>();

        PubSubCommandHandler<K, V> handler = new PubSubCommandHandler<K, V>(queue, codec);
        RedisPubSubConnection<K, V> redisPubSubConnection = new RedisPubSubConnection<K, V>(queue, codec, timeout, unit);

        return connect(handler, redisPubSubConnection);
    }

    private <K, V, T extends RedisAsync<K, V>> T connect(CommandHandler<K, V> handler, T connection) {
        try {
            ConnectionWatchdog watchdog = new ConnectionWatchdog(clientBootstrap, channelGroup, timer);
            ChannelPipeline pipeline = Channels.pipeline(watchdog, handler, connection);
            Channel channel = clientBootstrap.getFactory().newChannel(pipeline);

            ChannelFuture future = channel.connect((SocketAddress) clientBootstrap.getOption("remoteAddress"));
            future.await();

            if (!future.isSuccess()) {
                throw future.getCause();
            }

            //auth passwd
            String passwd = (String)clientBootstrap.getOption("passwd");
            if(null != passwd){
            	String auth = connection.auth(passwd);
            	if(!"OK".equals(auth)){
            		throw new RedisException("auth fail,auth result:"+auth+",passwd:"+passwd);
            	}
            }
            
            watchdog.setReconnect(true);
            return connection;
        } catch (Throwable e) {
            throw new RedisException("Unable to connect", e);
        }
    }

    /**
     * Shutdown this client and close all open connections. The client should be
     * discarded after calling shutdown.
     */
    public void shutdown() {
        for (Channel c : channelGroup) {
            ChannelPipeline pipeline = c.getPipeline();
            RedisAsync<?, ?> connection = pipeline.get(RedisAsync.class);
            connection.close();
        }
        ChannelGroupFuture future = channelGroup.close();
        future.awaitUninterruptibly();
        clientBootstrap.releaseExternalResources();
    }
}

