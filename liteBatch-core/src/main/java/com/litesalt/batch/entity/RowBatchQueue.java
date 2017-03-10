package com.litesalt.batch.entity;

import java.util.List;

/**
 * @author Paul-xiong
 * @date 2017年2月26日
 * @description 批量插入队列
 */
public abstract class RowBatchQueue<T> {

	protected Class<T> clazz;

	public RowBatchQueue() {
		super();
	}

	public RowBatchQueue(Class<T> clazz) {
		super();
		if (clazz != null) {
			this.clazz = clazz;
		}
	}

	public abstract void put(T t);

	public abstract void put(List<T> ts);

	public abstract T take();

	public abstract List<T> take(long len);

	public abstract List<T> takeAll();

}
