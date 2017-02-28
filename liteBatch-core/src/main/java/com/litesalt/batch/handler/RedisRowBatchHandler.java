package com.litesalt.batch.handler;

import org.springframework.jdbc.core.JdbcTemplate;

import com.litesalt.batch.entity.RedisRowBatchQueue;

/**
 * @author Paul-xiong
 * @date 2017年2月27日
 * @description redis批插处理器
 */
public class RedisRowBatchHandler<T> extends RowBatchHandler<T> {

	public RedisRowBatchHandler(JdbcTemplate jdbcTemplate, int submitCapacity, Class<T> clazz, String host, int port) {
		super(jdbcTemplate, submitCapacity, clazz);
		this.queue = new RedisRowBatchQueue<T>(clazz, host, port);
	}

	public RedisRowBatchHandler(JdbcTemplate jdbcTemplate, int submitCapacity, Class<T> clazz, String host, int port,
			String redisKey) {
		super(jdbcTemplate, submitCapacity, clazz);
		this.queue = new RedisRowBatchQueue<T>(clazz, host, port, redisKey);
	}
}
