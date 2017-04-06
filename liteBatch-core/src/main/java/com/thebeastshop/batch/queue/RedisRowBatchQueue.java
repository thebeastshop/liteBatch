package com.thebeastshop.batch.queue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.thebeastshop.batch.context.QueueContext;

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

	private String key;

	/**
	 * 生成redis队列key
	 * 
	 * @return
	 */
	private void buildKey() {
		Class<T> clazz = context.getClazz();
		StringBuilder stringBuilder = new StringBuilder("ROW_BATCH_QUEUE_").append(clazz.getSimpleName().toUpperCase());
		if (context != null) {
			if (context.getType() != null) {
				stringBuilder.append("_").append(context.getType().toString().toUpperCase());
			}
			if (StringUtils.isNotBlank(context.getRedisKeyExt())) {
				stringBuilder.append("_").append(context.getRedisKeyExt().toUpperCase());
			}
		}
		key = stringBuilder.toString();
	}

	// ==================================
	public RedisRowBatchQueue() {
		this(DEFAULT_HOST, DEFAULT_PORT, null);
	}

	public RedisRowBatchQueue(String host, int port, String auth) {
		this(new QueueContext<T>(), host, port, auth);
	}

	public RedisRowBatchQueue(QueueContext<T> context, String host, int port, String auth) {
		super(context);
		JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
		jedisPoolConfig.setMaxTotal(100);
		this.jedisPool = new JedisPool(jedisPoolConfig, host, port, 3000, auth);
		this.buildKey();
	}

	@Override
	public void put(T t) {
		put(Arrays.asList(t));
	}

	@Override
	public void put(List<T> ts) {
		if (ts != null && ts.size() > 0) {
			Jedis jedis = this.jedisPool.getResource();
			try {
				Pipeline pipe = jedis.pipelined();
				for (T t : ts) {
					String value = JSONObject.toJSONString(t);
					pipe.rpush(key, value);
				}
				pipe.sync();
			} catch (Exception e) {
				this.logger.error("Redis exception: {}", e.getMessage(), e);
			} finally {
				if (jedis != null) {
					jedis.close();
				}
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
				jedis = this.jedisPool.getResource();
				Pipeline pipe = jedis.pipelined();
				List<Response<String>> responseList = new ArrayList<Response<String>>();
				while (len > 0) {
					responseList.add(pipe.lpop(key));
					len--;
				}
				pipe.sync();
				String item = null;
				for (Response<String> response : responseList) {
					item = response.get();
					if (StringUtils.isNotBlank(item)) {
						rt.add(JSONObject.parseObject(item, context.getClazz()));
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
			Long len = jedis.llen(key);
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
