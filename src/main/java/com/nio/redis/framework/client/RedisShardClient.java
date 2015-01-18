package com.nio.redis.framework.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import redis.clients.util.Hashing;
import redis.clients.util.SafeEncoder;

public class RedisShardClient {
    private String connectionStr = null;
    private List<RedisClient> redisShardClientList = new ArrayList<RedisClient>();
    private final Hashing algo= Hashing.MURMUR_HASH;
    private TreeMap<Long, Integer> nodes =null;
    
    public RedisShardClient() {
    	
	}
    
	public RedisShardClient(String connectionStr) {
		super();
		this.connectionStr = connectionStr;
	}



	public void init(){
        if(null == this.connectionStr || this.connectionStr.isEmpty())
        	throw new ExceptionInInitializerError("connectionStr is empty!");
		List<String> confList = Arrays.asList(this.connectionStr.split("(?:\\s|,)+"));
		if (null == confList || confList.isEmpty()) 
			throw new ExceptionInInitializerError("confList is empty!");
		this.nodes = new TreeMap<Long, Integer>();
		for (int i=0;i<confList.size();i++) {
			String address = confList.get(i);
            if (address != null) {
                String[] addressArr = address.split(":");
                if (addressArr.length == 1) throw new ExceptionInInitializerError(addressArr + " is not include host:port or host:port:passwd after split \":\"");
                String host = addressArr[0];
                int port = Integer.valueOf(addressArr[1]);
                String passwd = null;
                
                //generate client
                RedisClient redisClient = null;
                if (addressArr.length == 3 && !addressArr[2].isEmpty()) {
                	passwd = addressArr[2];
                	redisClient = new RedisClient(host,port,passwd);
                }else{
                	redisClient = new RedisClient(host,port);
                }
                
                //shard
                for (int n = 0; n < 160 ; n++) {
                    this.nodes.put(algo.hash("SHARD-" + i + "-NODE-" + n), i);
                }
                
                //add client to fixed index list
                this.redisShardClientList.add(i, redisClient);
            }
        }
		
        
	}
	
	public void destroy() throws Exception{
		if(null != this.redisShardClientList){
			Iterator<RedisClient> iterator = this.redisShardClientList.iterator();
			while (iterator.hasNext()){
				try{
					RedisClient redisClient = iterator.next();
		        	redisClient.shutdown();
		        }catch(Exception e){
		        	e.printStackTrace();
		        	throw e;
		        }
		        
		      }
		}
	}
	
	/**
	 * borrow Redis 1st, then using
	 * bowwowed redis is a net connected object, if not, null returned 
	 * @param shardKey
	 * @return
	 */
	private Redis<String, String> borrowShardRedis(String shardKey){
		if(shardKey instanceof String){
            byte[] shardKeyByte = SafeEncoder.encode(shardKey);
            int index =0;
            SortedMap<Long, Integer> tail = nodes.tailMap(algo.hash(shardKeyByte));
            if (tail.size() == 0) {
                index = this.nodes.get(nodes.firstKey());
            }else{
            	index = tail.get(tail.firstKey());
            }
            
            RedisClient redisClient = this.redisShardClientList.get(index);
            Redis<String, String> redis = null;
            if(null != redisClient){
            	redis = redisClient.connect();
            }
            return redis;
        }
		
        return null;
        
	}
	
	private void returnShardRedis(Redis<String, String> redis){
        redis.close();
	}
	
	/**
	 * set
	 * @param key
	 * @param seconds
	 * @param value
	 * @return
	 * @throws RedisAccessException
	 */
	public List<Object> set(String key, int seconds, String value) throws Exception {
        List<Object> result = new ArrayList<Object>();
        Redis<String,String> redis = borrowShardRedis(key);
        try {
            String rst1 = redis.set(key, value);
            redis.setTimeout(seconds, TimeUnit.SECONDS);
            result.add(rst1);
        } catch (Exception ex) {
            throw new Exception("err happend in method set,",ex);
        } finally {
        	if(null != redis){
        		returnShardRedis(redis);
        	}
        }
        return result;
    }
	
	/**
	 * 
	 * @param key
	 * @return
	 * @throws Exception
	 */
	public Long del(String key) throws Exception {
        Redis<String,String> redis = borrowShardRedis(key);
        try {
            return redis.del(key);
        } catch (Exception ex) {
            throw new Exception("err happend in method del,",ex);
        } finally {
        	if(null != redis){
        		returnShardRedis(redis);
        	}
        }
    }
	
	
	/**
	 * 
	 * @param key
	 * @param map
	 * @return
	 * @throws Exception
	 */
	public String hmset(String key, Map<String, String> map) throws Exception{
        Redis<String,String> redis = borrowShardRedis(key);
	    try {
	            return redis.hmset(key, map);
	    } catch (Exception ex) {
	            throw new Exception("err happend in method hmset,",ex);
	    } finally {
	        	if(null != redis){
	        		returnShardRedis(redis);
	        	}
	    }
		 
	 }
	
	/**
	 * 
	 * @param key
	 * @param fields
	 * @return
	 * @throws Exception
	 */
	public List<String> hmget(String key, String[] fields) throws Exception{
        Redis<String,String> redis = borrowShardRedis(key);
	        try {
	            return redis.hmget(key, fields);
	        } catch (Exception ex) {
	            throw new Exception("err happend in method hmget,",ex);
	        } finally {
	        	if(null != redis){
	        		returnShardRedis(redis);
	        	}
	        }
		 
	 }
	
	
	/**
	 * 
	 * @param key
	 * @return
	 * @throws Exception
	 */
	public Map<String, String> hgetall(String key)throws Exception{
        Redis<String,String> redis = borrowShardRedis(key);
        try {
            return redis.hgetall(key);
        } catch (Exception ex) {
            throw new Exception("err happend in method hgetall,",ex);
        } finally {
        	if(null != redis){
        		returnShardRedis(redis);
        	}
        }
	 
 }
	
	/**
	 * 
	 * @param key
	 * @param score
	 * @param member
	 * @return
	 * @throws Exception
	 */
	public Long zadd(String key, double score, String member) throws Exception{
        Redis<String,String> redis = borrowShardRedis(key);
	        try {
	            return redis.zadd(key,score,member);
	        } catch (Exception ex) {
	            throw new Exception("err happend in method zadd,",ex);
	        } finally {
	        	if(null != redis){
	        		returnShardRedis(redis);
	        	}
	        }
		 
	 }
	
	/**
	 * 
	 * @param key
	 * @param member
	 * @return
	 * @throws Exception
	 */
	public Long zrem(String key, String member) throws Exception{
		 Redis<String,String> redis = borrowShardRedis(key);
	        try {
	            return redis.zrem(key, member);
	        } catch (Exception ex) {
	            throw new Exception("err happend in method zrem,",ex);
	        } finally {
	        	if(null != redis){
	        		returnShardRedis(redis);
	        	}
	        }
	}
	
	
	public List<String> zrangebyscore(String key, String min, String max, long offset, long count)throws Exception{
		 Redis<String,String> redis = borrowShardRedis(key);
	        try {
	            return redis.zrangebyscore(key, min, max, offset, count);
	        } catch (Exception ex) {
	            throw new Exception("err happend in method zrem,",ex);
	        } finally {
	        	if(null != redis){
	        		returnShardRedis(redis);
	        	}
	        }
	}
	
	


	public String getConnectionStr() {
		return connectionStr;
	}

	public void setConnectionStr(String connectionStr) {
		this.connectionStr = connectionStr;
	}


}
