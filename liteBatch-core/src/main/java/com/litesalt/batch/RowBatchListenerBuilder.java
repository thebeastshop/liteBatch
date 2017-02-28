package com.litesalt.batch;

import org.springframework.jdbc.core.JdbcTemplate;

import com.litesalt.batch.handler.RedisRowBatchHandler;
import com.litesalt.batch.listener.RowBatchListener;

/**
 * @author Paul-xiong
 * @date 2017年2月27日
 * @description 批插监听管理器构造器
 */
public class RowBatchListenerBuilder {

	/**
	 * 构建内存批插监听管理器
	 * 
	 * @param jdbcTemplate
	 * @param submitCapacity
	 * @param clazz
	 * @return
	 */
	public static <T> RowBatchListener<T> buildMemoryRowBatchListener(JdbcTemplate jdbcTemplate, int submitCapacity,
			Class<T> clazz) {
		RowBatchListener<T> listener = new RowBatchListener<>(jdbcTemplate, submitCapacity, clazz);
		return listener;
	}

	/**
	 * 构建redis批插监听管理器
	 * 
	 * @param jdbcTemplate
	 * @param submitCapacity
	 * @param clazz
	 * @param host
	 * @param port
	 * @return
	 */
	public static <T> RowBatchListener<T> buildRedisRowBatchListener(JdbcTemplate jdbcTemplate, int submitCapacity,
			Class<T> clazz, String host, int port) {
		RowBatchListener<T> listener = new RowBatchListener<>(jdbcTemplate, submitCapacity, clazz);
		listener.setRowBatchHandler(new RedisRowBatchHandler<>(jdbcTemplate, submitCapacity, clazz, host, port));
		return listener;
	}

	/**
	 * 构建redis批插监听管理器
	 * 
	 * @param jdbcTemplate
	 * @param submitCapacity
	 * @param clazz
	 * @param host
	 * @param port
	 * @param redisKey
	 * @return
	 */
	public static <T> RowBatchListener<T> buildRedisRowBatchListener(JdbcTemplate jdbcTemplate, int submitCapacity,
			Class<T> clazz, String host, int port, String redisKey) {
		RowBatchListener<T> listener = new RowBatchListener<>(jdbcTemplate, submitCapacity, clazz);
		listener.setRowBatchHandler(
				new RedisRowBatchHandler<>(jdbcTemplate, submitCapacity, clazz, host, port, redisKey));
		return listener;
	}

}
