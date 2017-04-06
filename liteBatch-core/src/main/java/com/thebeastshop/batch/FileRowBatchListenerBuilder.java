package com.thebeastshop.batch;

import java.io.File;

import com.thebeastshop.batch.context.HandlerContext;
import com.thebeastshop.batch.context.QueueContext;
import com.thebeastshop.batch.enums.FileSavedCapacity;
import com.thebeastshop.batch.enums.TargetType;
import com.thebeastshop.batch.handler.FileRowBatchHandler;
import com.thebeastshop.batch.listener.RowBatchListener;
import com.thebeastshop.batch.queue.MemoryRowBatchQueue;
import com.thebeastshop.batch.queue.RedisRowBatchQueue;

/**
 * @author Paul-xiong
 * @date 2017年2月27日
 * @description 批插监听管理器构造器
 */
public class FileRowBatchListenerBuilder {

	/**
	 * 构建内存批插监听管理器
	 * 
	 * @param file
	 * @param submitCapacity
	 * @param clazz
	 * @param capacity
	 * @return
	 */
	public static <T> RowBatchListener<T> buildMemoryRowBatchListener(File file, long submitCapacity, Class<T> clazz, FileSavedCapacity capacity) {
		return buildMemoryRowBatchListener(file, submitCapacity, clazz, capacity, false);
	}

	/**
	 * 构建内存批插监听管理器
	 * 
	 * @param file
	 * @param submitCapacity
	 * @param clazz
	 * @param capacity
	 * @param syn
	 * @return
	 */
	public static <T> RowBatchListener<T> buildMemoryRowBatchListener(File file, long submitCapacity, Class<T> clazz, FileSavedCapacity capacity, boolean syn) {
		QueueContext<T> qContext = new QueueContext<T>(TargetType.FILE);
		MemoryRowBatchQueue<T> queue = new MemoryRowBatchQueue<T>(qContext);
		HandlerContext<T> hContext = new HandlerContext<>(queue, submitCapacity, clazz, syn);
		FileRowBatchHandler<T> rowBatchHandler = new FileRowBatchHandler<>(hContext, file, capacity);
		RowBatchListener<T> listener = new RowBatchListener<>(rowBatchHandler);
		return listener;
	}

	/**
	 * 构建redis批插监听管理器
	 * 
	 * @param file
	 * @param submitCapacity
	 * @param clazz
	 * @param capacity
	 * @param host
	 * @param port
	 * @param auth
	 * @return
	 */
	public static <T> RowBatchListener<T> buildRedisRowBatchListener(File file, long submitCapacity, Class<T> clazz, FileSavedCapacity capacity, String host, int port, String auth) {
		return buildRedisRowBatchListener(file, submitCapacity, clazz, capacity, host, port, auth, false);
	}

	/**
	 * 构建redis批插监听管理器
	 * 
	 * @param file
	 * @param submitCapacity
	 * @param clazz
	 * @param host
	 * @param port
	 * @param auth
	 * @param syn
	 * @return
	 */
	public static <T> RowBatchListener<T> buildRedisRowBatchListener(File file, long submitCapacity, Class<T> clazz, FileSavedCapacity capacity, String host, int port, String auth, boolean syn) {
		QueueContext<T> qContext = new QueueContext<T>(TargetType.FILE, clazz, file.getName());
		RedisRowBatchQueue<T> queue = new RedisRowBatchQueue<T>(qContext, host, port, auth);
		HandlerContext<T> hContext = new HandlerContext<>(queue, submitCapacity, clazz, syn);
		FileRowBatchHandler<T> rowBatchHandler = new FileRowBatchHandler<T>(hContext, file, capacity);
		RowBatchListener<T> listener = new RowBatchListener<T>(rowBatchHandler);
		return listener;
	}

}
