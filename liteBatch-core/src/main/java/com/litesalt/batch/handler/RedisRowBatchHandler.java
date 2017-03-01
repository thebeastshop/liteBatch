package com.litesalt.batch.handler;

import org.springframework.jdbc.core.JdbcTemplate;

import com.litesalt.batch.entity.RedisRowBatchQueue;

/**
 * @author Paul-xiong
 * @date 2017年2月27日
 * @description redis批插处理器
 */
public class RedisRowBatchHandler<T> extends RowBatchHandler<T> {

	public RedisRowBatchHandler(JdbcTemplate jdbcTemplate, long submitCapacity, Class<T> clazz, String host, int port) {
		super(jdbcTemplate, submitCapacity, clazz);
		this.queue = new RedisRowBatchQueue<T>(clazz, host, port);
	}
}
