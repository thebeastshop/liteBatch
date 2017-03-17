package com.litesalt.batch;

import java.io.File;

import com.litesalt.batch.context.QueueContext;
import com.litesalt.batch.enums.FileSavedCapacity;
import com.litesalt.batch.enums.TargetType;
import com.litesalt.batch.handler.FileRowBatchHandler;
import com.litesalt.batch.listener.RowBatchListener;
import com.litesalt.batch.queue.MemoryRowBatchQueue;
import com.litesalt.batch.queue.RedisRowBatchQueue;

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
		QueueContext context = new QueueContext(TargetType.FILE);
		MemoryRowBatchQueue<T> queue = new MemoryRowBatchQueue<T>(context);
		FileRowBatchHandler<T> rowBatchHandler = new FileRowBatchHandler<>(file, queue, submitCapacity, clazz, capacity);
		RowBatchListener<T> listener = new RowBatchListener<>(rowBatchHandler);
		return listener;
	}

	/**
	 * 构建redis批插监听管理器
	 * 
	 * @param file
	 * @param submitCapacity
	 * @param clazz
	 * @param host
	 * @param port
	 * @return
	 */
	public static <T> RowBatchListener<T> buildRedisRowBatchListener(File file, long submitCapacity, Class<T> clazz, FileSavedCapacity capacity, String host, int port, String auth) {
		QueueContext context = new QueueContext(TargetType.FILE, file.getName());
		RedisRowBatchQueue<T> queue = new RedisRowBatchQueue<T>(clazz, context, host, port, auth);
		FileRowBatchHandler<T> rowBatchHandler = new FileRowBatchHandler<T>(file, queue, submitCapacity, clazz, capacity);
		RowBatchListener<T> listener = new RowBatchListener<T>(rowBatchHandler);
		return listener;
	}

}
