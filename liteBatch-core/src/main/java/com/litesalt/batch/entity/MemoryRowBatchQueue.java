package com.litesalt.batch.entity;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

/**
 * @author Paul-xiong
 * @date 2017年2月26日
 * @description 内存插入队列
 */
public class MemoryRowBatchQueue<T> extends RowBatchQueue<T> {

	private final Logger logger = Logger.getLogger(MemoryRowBatchQueue.class);

	private BlockingQueue<T> items;

	public MemoryRowBatchQueue() {
		this(1024 * 1024);
	}

	public MemoryRowBatchQueue(Integer queueCapacity) {
		super(null);
		items = new LinkedBlockingQueue<T>(queueCapacity);
	}

	@Override
	public void put(T t) {
		try {
			items.put(t);
		} catch (InterruptedException e) {
			logger.error("put is interrupted", e);
		}
	}

	@Override
	public T take() {
		try {
			return items.take();
		} catch (InterruptedException e) {
			logger.error("take is interrupted", e);
			return null;
		}
	}

	@Override
	public List<T> take(long len) {
		List<T> rt = new ArrayList<T>();
		try {
			while (len > 0) {
				T item = items.take();
				if (item != null) {
					rt.add(item);
				}
				len--;
			}
		} catch (InterruptedException e) {
			logger.error("take is interrupted", e);
		}
		return rt;
	}

	@Override
	public List<T> takeAll() {
		return take(items.size());
	}

}
