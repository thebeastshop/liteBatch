package com.litesalt.batch.queue;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author Paul-xiong
 * @date 2017年2月26日
 * @description 内存插入队列
 */
public class MemoryRowBatchQueue<T> extends RowBatchQueue<T> {

	private Queue<T> items;

	public MemoryRowBatchQueue() {
		this(1024 * 1024);
	}

	public MemoryRowBatchQueue(Integer queueCapacity) {
		super(null);
		items = new ConcurrentLinkedQueue<T>();
	}

	@Override
	public void put(T t) {
		items.add(t);
	}

	@Override
	public void put(List<T> ts) {
		if (ts != null && ts.size() > 0) {
			for (T t : ts) {
				put(t);
			}
		}
	}

	@Override
	public T take() {
		return items.poll();
	}

	@Override
	public List<T> take(long len) {
		List<T> rt = new ArrayList<T>();
		while (len > 0) {
			T item = take();
			if (item != null) {
				rt.add(item);
			}
			len--;
		}
		return rt;
	}

	@Override
	public List<T> takeAll() {
		return take(items.size());
	}

}
