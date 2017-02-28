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

	private final static String DEFAULT_HOST = "localhost";

	private final static int DEFAULT_PORT = 6379;

	private final static String DEFAULT_REDIS_KEY = "rowBatchQueue";

	private final Logger logger = LoggerFactory.getLogger(RedisRowBatchQueue.class);

	private JedisPool jedisPool;

	private String redisKey;

	// ==================================
	public RedisRowBatchQueue(Class<T> clazz) {
		this(clazz, DEFAULT_HOST, DEFAULT_PORT);
	}

	public RedisRowBatchQueue(Class<T> clazz, String host, int port) {
		this(clazz, host, port, DEFAULT_REDIS_KEY);
	}

	public RedisRowBatchQueue(Class<T> clazz, String host, int port, String redisKey) {
		super(clazz);
		this.jedisPool = new JedisPool(new JedisPoolConfig(), host, port, 3000);
		this.redisKey = redisKey;
	}

	@Override
	public void put(T t) {
		try (Jedis jedis = this.jedisPool.getResource()) {
			String value = JSONObject.toJSONString(t);
			this.logger.debug("start: rpush redis. key={}, value={}", redisKey, value);
			jedis.rpush(redisKey, value);
		} catch (Exception e) {
			this.logger.error("Redis exception: {}", e.getMessage(), e);
		}
	}

	@Override
	public T take() {
		try (Jedis jedis = this.jedisPool.getResource()) {
			String value = jedis.lpop(redisKey);
			return JSONObject.parseObject(value, clazz);
		} catch (Exception e) {
			this.logger.error("Redis exception: {}", e.getMessage(), e);
			return null;
		}
	}

	@Override
	public List<T> take(long len) {
		List<T> rt = new ArrayList<T>();
		try (Jedis jedis = this.jedisPool.getResource()) {
			if (len > 0) {
				while (len > 0) {
					T item = take();
					if (item != null) {
						rt.add(item);
					}
					len--;
				}
			}
		} catch (Exception e) {
			this.logger.error("Redis exception: {}", e.getMessage(), e);
		}
		return rt;
	}

	@Override
	public List<T> takeAll() {
		try (Jedis jedis = this.jedisPool.getResource()) {
			Long len = jedis.llen(redisKey);
			return take(len != null ? len : 0);
		} catch (Exception e) {
			this.logger.error("Redis exception: {}", e.getMessage(), e);
			return new ArrayList<T>();
		}
	}

}
