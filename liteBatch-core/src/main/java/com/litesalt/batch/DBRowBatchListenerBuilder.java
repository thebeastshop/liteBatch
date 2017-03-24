package com.litesalt.batch;

import org.springframework.jdbc.core.JdbcTemplate;

import com.litesalt.batch.context.HandlerContext;
import com.litesalt.batch.context.QueueContext;
import com.litesalt.batch.enums.TargetType;
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
		return buildMemoryRowBatchListener(jdbcTemplate, submitCapacity, clazz, false);
	}

	/**
	 * 构建内存批插监听管理器
	 * 
	 * @param jdbcTemplate
	 * @param submitCapacity
	 * @param clazz
	 * @param syn
	 * @return
	 */
	public static <T> RowBatchListener<T> buildMemoryRowBatchListener(JdbcTemplate jdbcTemplate, long submitCapacity, Class<T> clazz, boolean syn) {
		MemoryRowBatchQueue<T> queue = new MemoryRowBatchQueue<T>();
		HandlerContext<T> context = new HandlerContext<T>(queue, submitCapacity, clazz, syn);
		DBRowBatchHandler<T> rowBatchHandler = new DBRowBatchHandler<T>(context, jdbcTemplate);
		RowBatchListener<T> listener = new RowBatchListener<T>(rowBatchHandler);
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
	 * @param auth
	 * @return
	 */
	public static <T> RowBatchListener<T> buildRedisRowBatchListener(JdbcTemplate jdbcTemplate, long submitCapacity, Class<T> clazz, String host, int port, String auth) {
		return buildRedisRowBatchListener(jdbcTemplate, submitCapacity, clazz, host, port, auth, false);
	}

	/**
	 * 构建redis批插监听管理器
	 * 
	 * @param jdbcTemplate
	 * @param submitCapacity
	 * @param clazz
	 * @param host
	 * @param port
	 * @param syn
	 * @return
	 */
	public static <T> RowBatchListener<T> buildRedisRowBatchListener(JdbcTemplate jdbcTemplate, long submitCapacity, Class<T> clazz, String host, int port, String auth, boolean syn) {
		QueueContext<T> qContext = new QueueContext<T>(TargetType.DB, clazz);
		RedisRowBatchQueue<T> queue = new RedisRowBatchQueue<T>(qContext, host, port, auth);
		HandlerContext<T> context = new HandlerContext<>(queue, submitCapacity, clazz, syn);
		DBRowBatchHandler<T> rowBatchHandler = new DBRowBatchHandler<T>(context, jdbcTemplate);
		RowBatchListener<T> listener = new RowBatchListener<T>(rowBatchHandler);
		return listener;
	}

}
