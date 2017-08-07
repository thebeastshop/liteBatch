/**
 * <p>Title: liteBatch</p>
 * <p>Description: 一个轻量级，高性能的快速批插工具</p>
 * <p>Copyright: Copyright (c) 2016</p>
 * @author Bryan.Zhang
 * @email 47483522@qq.com
 * @Date 2016-11-10
 * @version 1.0
 */
package com.thebeastshop.batch.handler;

import java.util.List;
import java.util.Observable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.thebeastshop.batch.callback.ExceptionCallback;
import com.thebeastshop.batch.context.HandlerContext;
import com.thebeastshop.batch.monitor.QueueStatusMonitor;
import com.thebeastshop.batch.queue.RowBatchQueue;

/**
 * 批插处理器
 */
public abstract class RowBatchHandler<T> extends Observable {

	protected final Logger logger = Logger.getLogger(RowBatchHandler.class);

	protected AtomicLong loopSize = new AtomicLong(0);

	protected HandlerContext<T> context;

	protected ExceptionCallback<T> exceptionCallback;

	private ExecutorService threadPool = Executors.newFixedThreadPool(10);

	// ========================================

	public RowBatchHandler(HandlerContext<T> context) {
		super();
		// ======添加观察者=====
		this.addObserver(new QueueStatusMonitor<T>(this, context.getMonitorTime()));
		// ===================
		this.context = context;
		this.exceptionCallback = context.getExceptionCallback();
	}

	public abstract void rowBatch(final List<T> batchList);

	public void insertWithBatch(List<T> items) {
		try {
			RowBatchQueue<T> queue = context.getQueue();
			if (queue != null && items != null && items.size() > 0) {
				queue.put(items);
				if (!context.isSyn()) {
					loopSize.addAndGet(items.size());
					final long submitCapacity = context.getSubmitCapacity();
					if (loopSize.get() >= submitCapacity) {
						threadPool.submit(new Thread() {
							@Override
							public void run() {
								try {
									rowBatch(take(submitCapacity));
								} catch (Exception e) {
									logger.error("批次插入发生异常", e);
								}
							}
						});
						loopSize.set(0);
						// 向观察者发送通知
						this.setChanged();
						this.notifyObservers();
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void flush() {
		rowBatch(takeAll());
	}

	public List<T> take(long len) {
		try {
			RowBatchQueue<T> queue = context.getQueue();
			if (queue != null) {
				return queue.take(len);
			} else {
				return null;
			}
		} catch (Exception e) {
			logger.error("take is interrupted", e);
			return null;
		}
	}

	public List<T> takeAll() {
		try {
			RowBatchQueue<T> queue = context.getQueue();
			if (queue != null) {
				return queue.takeAll();
			} else {
				return null;
			}
		} catch (Exception e) {
			logger.error("take is interrupted", e);
			return null;
		}
	}
}
