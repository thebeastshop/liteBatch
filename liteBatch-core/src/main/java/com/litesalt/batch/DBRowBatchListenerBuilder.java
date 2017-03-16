package com.litesalt.batch;

import org.springframework.jdbc.core.JdbcTemplate;

import com.litesalt.batch.handler.MemoryDBRowBatchHandler;
import com.litesalt.batch.handler.RedisDBRowBatchHandler;
import com.litesalt.batch.listener.RowBatchListener;

/**
 * @author Paul-xiong
 * @date 2017年2月27日
 * @description 批插监听管理器构造器
 */
public class DBRowBatchListenerBuilder {

	/**
	 * 构建内存批插监听管理器
	 * 
	 * @param jdbcTemplate
	 * @param submitCapacity
	 * @param clazz
	 * @return
	 */
	public static <T> RowBatchListener<T> buildMemoryRowBatchListener(JdbcTemplate jdbcTemplate, long submitCapacity, Class<T> clazz) {
		RowBatchListener<T> listener = new RowBatchListener<>();
		listener.setRowBatchHandler(new MemoryDBRowBatchHandler<>(jdbcTemplate, submitCapacity, clazz));
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
	public static <T> RowBatchListener<T> buildRedisRowBatchListener(JdbcTemplate jdbcTemplate, long submitCapacity, Class<T> clazz, String host,
			int port,String auth) {
		RowBatchListener<T> listener = new RowBatchListener<T>();
		listener.setRowBatchHandler(new RedisDBRowBatchHandler<T>(jdbcTemplate, submitCapacity, clazz, host, port,auth));
		return listener;
	}

}
