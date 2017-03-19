package com.litesalt.batch;

import org.springframework.jdbc.core.JdbcTemplate;

import com.litesalt.batch.context.HandlerContext;
import com.litesalt.batch.handler.DBRowBatchHandler;
import com.litesalt.batch.listener.RowBatchListener;
import com.litesalt.batch.queue.MemoryRowBatchQueue;
import com.litesalt.batch.queue.RedisRowBatchQueue;

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
		MemoryRowBatchQueue<T> queue = new MemoryRowBatchQueue<T>();
		HandlerContext<T> context = new HandlerContext<>(queue, submitCapacity, clazz);
		DBRowBatchHandler<T> rowBatchHandler = new DBRowBatchHandler<>(context, jdbcTemplate);
		RowBatchListener<T> listener = new RowBatchListener<>(rowBatchHandler);
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
	public static <T> RowBatchListener<T> buildRedisRowBatchListener(JdbcTemplate jdbcTemplate, long submitCapacity, Class<T> clazz, String host, int port, String auth) {
		RedisRowBatchQueue<T> queue = new RedisRowBatchQueue<T>(clazz, host, port, auth);
		HandlerContext<T> context = new HandlerContext<>(queue, submitCapacity, clazz);
		DBRowBatchHandler<T> rowBatchHandler = new DBRowBatchHandler<T>(context, jdbcTemplate);
		RowBatchListener<T> listener = new RowBatchListener<T>(rowBatchHandler);
		return listener;
	}

}
