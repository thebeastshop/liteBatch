package com.litesalt.batch.context;

import com.litesalt.batch.queue.MemoryRowBatchQueue;
import com.litesalt.batch.queue.RowBatchQueue;

/**
 * @author Paul-xiong
 * @date 2017年3月17日
 * @description 批插操作器上下文
 */
public class HandlerContext<T> {

	/**
	 * 默认提交数量
	 */
	private final static long DEFAULT_SUBMIT_CAPACITY = 5000;
	/**
	 * 缓存队列
	 */
	private RowBatchQueue<T> queue;
	/**
	 * 触发异步插入的提交数量
	 */
	private long submitCapacity;
	/**
	 * 操作的泛型类
	 */
	private Class<T> clazz;
	/**
	 * 是否同步批插
	 */
	private boolean syn;

	public HandlerContext() {
		this(new MemoryRowBatchQueue<T>(), null);
	}

	public HandlerContext(RowBatchQueue<T> queue, Class<T> clazz) {
		this(queue, DEFAULT_SUBMIT_CAPACITY, clazz);
	}

	public HandlerContext(RowBatchQueue<T> queue, long submitCapacity, Class<T> clazz) {
		this(queue, submitCapacity, clazz, false);
	}

	public HandlerContext(RowBatchQueue<T> queue, long submitCapacity, Class<T> clazz, boolean syn) {
		super();
		this.queue = queue;
		this.submitCapacity = submitCapacity;
		this.clazz = clazz;
		this.syn = syn;
	}

	public RowBatchQueue<T> getQueue() {
		return queue;
	}

	public void setQueue(RowBatchQueue<T> queue) {
		this.queue = queue;
	}

	public long getSubmitCapacity() {
		return submitCapacity;
	}

	public void setSubmitCapacity(long submitCapacity) {
		this.submitCapacity = submitCapacity;
	}

	public Class<T> getClazz() {
		return clazz;
	}

	public void setClazz(Class<T> clazz) {
		this.clazz = clazz;
	}

	public boolean isSyn() {
		return syn;
	}

	public void setSyn(boolean syn) {
		this.syn = syn;
	}

}
