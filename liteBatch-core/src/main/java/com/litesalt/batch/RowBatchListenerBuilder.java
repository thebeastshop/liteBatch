package com.litesalt.batch;

import org.springframework.jdbc.core.JdbcTemplate;

import com.litesalt.batch.handler.RedisRowBatchHandler;
import com.litesalt.batch.listener.RowBatchListener;

/**
 * 批量插入队列构造器
 * 
 * @author Paul-xiong
 * @date 2017年2月27日
 * @description
 */
public class RowBatchListenerBuilder {

	public static <T> RowBatchListener<T> buildMemoryRowBatchListener(JdbcTemplate jdbcTemplate, int submitCapacity,
			Class<T> clazz) {
		RowBatchListener<T> listener = new RowBatchListener<>(jdbcTemplate, submitCapacity, clazz);
		return listener;
	}

	public static <T> RowBatchListener<T> buildRedisRowBatchListener(JdbcTemplate jdbcTemplate, int submitCapacity,
			Class<T> clazz, String host, int port) {
		RowBatchListener<T> listener = new RowBatchListener<>(jdbcTemplate, submitCapacity, clazz);
		listener.setRowBatchHandler(new RedisRowBatchHandler<>(jdbcTemplate, submitCapacity, clazz, host, port));
		return listener;
	}

}
