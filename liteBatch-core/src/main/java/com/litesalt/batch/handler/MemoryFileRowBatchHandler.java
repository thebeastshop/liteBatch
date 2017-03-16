package com.litesalt.batch.handler;

import java.io.File;

import com.litesalt.batch.entity.MemoryRowBatchQueue;
import com.litesalt.batch.enums.FileSavedCapacity;

/**
 * @author Paul-xiong
 * @date 2017年2月27日
 * @description 内存批插处理器
 */
public class MemoryFileRowBatchHandler<T> extends FileRowBatchHandler<T> {

	public MemoryFileRowBatchHandler(File file, long submitCapacity, Class<T> clazz, FileSavedCapacity capacity) {
		super(file, submitCapacity, clazz, capacity);
		this.queue = new MemoryRowBatchQueue<T>();
	}
}
