package com.litesalt.batch.queue;

import java.util.List;

import com.litesalt.batch.context.QueueContext;

/**
 * @author Paul-xiong
 * @date 2017年2月26日
 * @description 批量插入队列
 */
public abstract class RowBatchQueue<T> {

	protected Class<T> clazz;

	protected QueueContext context;

	public RowBatchQueue() {
		this(null, null);
	}

	public RowBatchQueue(Class<T> clazz, QueueContext context) {
		super();
		this.clazz = clazz;
		this.context = context;
	}

	public abstract void put(T t);

	public abstract void put(List<T> ts);

	public abstract T take();

	public abstract List<T> take(long len);

	public abstract List<T> takeAll();

}
