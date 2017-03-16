/**
 * <p>Title: liteBatch</p>
 * <p>Description: 一个轻量级，高性能的快速批插工具</p>
 * <p>Copyright: Copyright (c) 2016</p>
 * @author Bryan.Zhang
 * @email 47483522@qq.com
 * @Date 2016-11-10
 * @version 1.0
 */
package com.litesalt.batch.handler;

import java.util.List;
import java.util.Observable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.litesalt.batch.QueueStatusMonitor;
import com.litesalt.batch.entity.RowBatchQueue;

/**
 * 批插处理器
 */
public abstract class RowBatchHandler<T> extends Observable {

	protected final Logger logger = Logger.getLogger(RowBatchHandler.class);

	protected RowBatchQueue<T> queue;

	protected AtomicLong loopSize = new AtomicLong(0);

	protected long submitCapacity;

	protected Class<T> clazz;

	private ExecutorService threadPool = Executors.newFixedThreadPool(10);

	// ========================================

	public RowBatchHandler(long submitCapacity, Class<T> clazz) {
		super();
		// ======添加观察者=====
		this.addObserver(new QueueStatusMonitor<T>(this));
		// ===================
		this.clazz = clazz;
		this.submitCapacity = submitCapacity;
	}

	public abstract void rowBatch(final List<T> batchList);

	public void insertWithBatch(List<T> items) {
		try {
			if (queue != null && items != null && items.size() > 0) {
				queue.put(items);
				loopSize.addAndGet(items.size());
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
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public List<T> take(long len) {
		try {
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

	public Class<T> getClazz() {
		return clazz;
	}
}
