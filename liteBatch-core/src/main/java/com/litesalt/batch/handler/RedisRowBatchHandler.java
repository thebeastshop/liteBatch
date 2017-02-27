package com.litesalt.batch.handler;

import org.springframework.jdbc.core.JdbcTemplate;

import com.litesalt.batch.entity.RedisRowBatchQueue;

/**
 * 内存批插处理器
 * 
 * @author Paul-xiong
 * @date 2017年2月27日
 * @description
 */
public class RedisRowBatchHandler<T> extends RowBatchHandler<T> {

	public RedisRowBatchHandler(JdbcTemplate jdbcTemplate, int submitCapacity, Class<T> clazz, String host, int port) {
		super(jdbcTemplate, submitCapacity, clazz);
		this.queue = new RedisRowBatchQueue<T>(clazz, host, port);
	}
}
