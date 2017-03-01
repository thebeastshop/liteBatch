package com.litesalt.batch.entity;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

/**
 * @author Paul-xiong
 * @date 2017年2月26日
 * @description redis插入队列
 */
public class RedisRowBatchQueue<T> extends RowBatchQueue<T> {

	private final static String DEFAULT_HOST = "localhost";

	private final static int DEFAULT_PORT = 6379;

	private final Logger logger = LoggerFactory.getLogger(RedisRowBatchQueue.class);

	private JedisPool jedisPool;

	/**
	 * 生成redis队列key
	 * 
	 * @return
	 */
	private String buildKey() {
		return new StringBuilder("ROW_BATCH_QUEUE_").append(clazz.getSimpleName().toUpperCase()).toString();
	}

	// ==================================
	public RedisRowBatchQueue(Class<T> clazz) {
		this(clazz, DEFAULT_HOST, DEFAULT_PORT);
	}

	public RedisRowBatchQueue(Class<T> clazz, String host, int port) {
		super(clazz);
		this.jedisPool = new JedisPool(new JedisPoolConfig(), host, port, 3000);
	}

	@Override
	public void put(T t) {
		Jedis jedis = null;
		try {
			String redisKey = buildKey();
			jedis = this.jedisPool.getResource();
			String value = JSONObject.toJSONString(t);
			this.logger.debug("start: rpush redis. key={}, value={}", redisKey, value);
			jedis.rpush(redisKey, value);
		} catch (Exception e) {
			this.logger.error("Redis exception: {}", e.getMessage(), e);
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}
	}

	@Override
	public T take() {
		List<T> take = take(1);
		if (take != null && take.size() > 0) {
			return take.get(0);
		}
		return null;
	}

	@Override
	public List<T> take(long len) {
		List<T> rt = new ArrayList<T>();
		Jedis jedis = null;
		try {
			if (len > 0) {
				String redisKey = buildKey();
				jedis = this.jedisPool.getResource();
				Pipeline pipe = jedis.pipelined();
				List<Response<String>> responseList = new ArrayList<Response<String>>();
				while (len > 0) {
					responseList.add(pipe.lpop(redisKey));
					len--;
				}
				pipe.sync();
				String item = null;
				for (Response<String> response : responseList) {
					item = response.get();
					if (StringUtils.isNotBlank(item)) {
						rt.add(JSONObject.parseObject(item, clazz));
					}
				}
			}
		} catch (Exception e) {
			this.logger.error("Redis exception: {}", e.getMessage(), e);
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}
		return rt;
	}

	@Override
	public List<T> takeAll() {
		Jedis jedis = null;
		try {
			jedis = this.jedisPool.getResource();
			String redisKey = buildKey();
			Long len = jedis.llen(redisKey);
			return take(len != null ? len : 0);
		} catch (Exception e) {
			this.logger.error("Redis exception: {}", e.getMessage(), e);
			return new ArrayList<T>();
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}
	}

}
