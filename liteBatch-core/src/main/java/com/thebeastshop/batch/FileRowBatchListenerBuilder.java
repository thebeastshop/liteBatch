package com.thebeastshop.batch;

import java.io.File;

import com.thebeastshop.batch.context.HandlerContext;
import com.thebeastshop.batch.context.QueueContext;
import com.thebeastshop.batch.enums.FileSavedCapacity;
import com.thebeastshop.batch.enums.TargetType;
import com.thebeastshop.batch.handler.FileRowBatchHandler;
import com.thebeastshop.batch.listener.RowBatchListener;
import com.thebeastshop.batch.queue.MemoryRowBatchQueue;

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

}
