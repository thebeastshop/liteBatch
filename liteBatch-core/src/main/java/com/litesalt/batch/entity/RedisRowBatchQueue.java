package com.litesalt.batch.entity;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author Paul-xiong
 * @date 2017年2月26日
 * @description redis插入队列
 */
public class RedisRowBatchQueue<T> extends RowBatchQueue<T> {

	private final Logger logger = LoggerFactory.getLogger(RedisRowBatchQueue.class);

	private JedisPool jedisPool;

	private String buildKey() {
		return "rowBatchQueue";
	}

	// ==================================
	public RedisRowBatchQueue(Class<T> clazz) {
		this(clazz, "localhost", 6379);
	}

	public RedisRowBatchQueue(Class<T> clazz, String host, int port) {
		super(clazz);
		this.jedisPool = new JedisPool(new JedisPoolConfig(), host, port, 3000);
	}

	@Override
	public void put(T t) {
		String key = buildKey();
		String value = JSONObject.toJSONString(t);
		this.logger.debug("start: rpush redis. key={}, value={}", key, value);
		try (Jedis jedis = this.jedisPool.getResource()) {
			jedis.lpush(key, value);
		} catch (Exception e) {
			this.logger.error("Redis exception: {}", e.getMessage(), e);
		}
	}

	@Override
	public T take() {
		String key = buildKey();
		try (Jedis jedis = this.jedisPool.getResource()) {
			String value = jedis.rpop(key);
			return (T) JSONObject.parseObject(value, clazz);
		} catch (Exception e) {
			this.logger.error("Redis exception: {}", e.getMessage(), e);
			return null;
		}
	}

	@Override
	public List<T> take(int len) {
		List<T> rt = new ArrayList<T>();
		while (len > 0) {
			T item = take();
			if (item != null) {
				rt.add(item);
			}
			len--;
		}
		return rt;
	}

}
