package com.yao.sparkgis.cache;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

public class RedisClient implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1843135356724325168L;
	//private Logger logger = Logger.getLogger(RedisClient.class);
	private RedisTemplate<String, byte[]> redisTemplate;
	
	public void setRedisTemplate(RedisTemplate<String, byte[]> redisTemplate) {
		this.redisTemplate = redisTemplate;
	}

	public boolean exists(final String key) {
		return redisTemplate.hasKey(key);
	}
	
	/**
	 * 写入缓存
	 */
	public boolean set(final String key, byte[] value) {
		boolean result = false;
		ValueOperations<String, byte[]> operations = redisTemplate.opsForValue();
		operations.set(key, value);
	//	logger.info("cache set succeed!");
		result = true;
		return result;
	}
	
	public boolean set (final String key, byte[] value, Long expireTime) {
		boolean result = false;
		ValueOperations<String, byte[]> operations = redisTemplate.opsForValue();
		operations.set(key, value);
		redisTemplate.expire(key, expireTime, TimeUnit.SECONDS);
		//logger.info("cache set succeed!");
		result = true;
		return result;
	}
	
	public Set<String> getSamePrefixKeys(String prefix) {
		return redisTemplate.keys(prefix);
	}
	
	/**
	 * 删除缓存
	 * @param key
	 * @return
	 */
	public void remove(final String key) {
		redisTemplate.delete(key);
		//logger.info("cache remove succeed");
	}
	
	public byte[] get(final String key) {
		ValueOperations<String, byte[]> operations = redisTemplate.opsForValue();
		return operations.get(key);
	}
}
