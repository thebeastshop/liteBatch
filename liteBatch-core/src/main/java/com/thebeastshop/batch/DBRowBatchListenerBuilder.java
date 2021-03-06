package com.thebeastshop.batch;

import org.springframework.jdbc.core.JdbcTemplate;

import com.thebeastshop.batch.callback.ExceptionCallback;
import com.thebeastshop.batch.context.HandlerContext;
import com.thebeastshop.batch.handler.DBRowBatchHandler;
import com.thebeastshop.batch.listener.RowBatchListener;
import com.thebeastshop.batch.queue.MemoryRowBatchQueue;

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
	public static <T> RowBatchListener<T> buildMemoryRowBatchListener(JdbcTemplate jdbcTemplate, Long submitCapacity, Class<T> clazz) {
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
	public static <T> RowBatchListener<T> buildMemoryRowBatchListener(JdbcTemplate jdbcTemplate, Long submitCapacity, Class<T> clazz, boolean syn) {
		return buildMemoryRowBatchListener(jdbcTemplate, submitCapacity, clazz, syn, null);
	}

	/**
	 * 构建内存批插监听管理器
	 * 
	 * @param jdbcTemplate
	 * @param submitCapacity
	 * @param clazz
	 * @param exceptionCallback
	 * @return
	 */
	public static <T> RowBatchListener<T> buildMemoryRowBatchListener(JdbcTemplate jdbcTemplate, Long submitCapacity, Class<T> clazz, ExceptionCallback<T> exceptionCallback) {
		return buildMemoryRowBatchListener(jdbcTemplate, submitCapacity, clazz, false, exceptionCallback);
	}

	/**
	 * 构建内存批插监听管理器
	 * 
	 * @param jdbcTemplate
	 * @param submitCapacity
	 * @param clazz
	 * @param exceptionCallback
	 * @param monitorTime
	 * @return
	 */
	public static <T> RowBatchListener<T> buildMemoryRowBatchListener(JdbcTemplate jdbcTemplate, Long submitCapacity, Class<T> clazz, ExceptionCallback<T> exceptionCallback, Long monitorTime) {
		return buildMemoryRowBatchListener(jdbcTemplate, submitCapacity, clazz, false, exceptionCallback, monitorTime);
	}

	/**
	 * 构建内存批插监听管理器
	 * 
	 * @param jdbcTemplate
	 * @param submitCapacity
	 * @param clazz
	 * @param syn
	 * @param exceptionCallback
	 * @return
	 */
	public static <T> RowBatchListener<T> buildMemoryRowBatchListener(JdbcTemplate jdbcTemplate, Long submitCapacity, Class<T> clazz, boolean syn, ExceptionCallback<T> exceptionCallback) {
		return buildMemoryRowBatchListener(jdbcTemplate, submitCapacity, clazz, syn, exceptionCallback, null);
	}

	/**
	 * 构建内存批插监听管理器
	 * 
	 * @param jdbcTemplate
	 * @param submitCapacity
	 * @param clazz
	 * @param syn
	 * @param exceptionCallback
	 * @param monitorTime
	 * @return
	 */
	public static <T> RowBatchListener<T> buildMemoryRowBatchListener(JdbcTemplate jdbcTemplate, Long submitCapacity, Class<T> clazz, boolean syn, ExceptionCallback<T> exceptionCallback, Long monitorTime) {
		MemoryRowBatchQueue<T> queue = new MemoryRowBatchQueue<T>();
		HandlerContext<T> context = new HandlerContext<>(queue, submitCapacity, clazz, syn, exceptionCallback, monitorTime);
		DBRowBatchHandler<T> rowBatchHandler = new DBRowBatchHandler<>(context, jdbcTemplate);
		RowBatchListener<T> listener = new RowBatchListener<>(rowBatchHandler);
		return listener;
	}

}
