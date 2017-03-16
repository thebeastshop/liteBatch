/**
 * <p>Title: liteBatch</p>
 * <p>Description: 一个轻量级，高性能的快速批插工具</p>
 * <p>Copyright: Copyright (c) 2016</p>
 * @author Bryan.Zhang
 * @email 47483522@qq.com
 * @Date 2016-11-10
 * @version 1.0
 */
package com.litesalt.batch.listener;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import com.litesalt.batch.handler.RowBatchHandler;

/**
 * 批插监听管理器
 */
public class RowBatchListener<T> {

	private final static Logger log = Logger.getLogger(RowBatchListener.class);

	private RowBatchHandler<T> rowBatchHandler;

	public RowBatchListener() {
		log.info("开始监听批次插入");
	}

	public RowBatchHandler<T> getRowBatchHandler() {
		return rowBatchHandler;
	}

	public void setRowBatchHandler(RowBatchHandler<T> rowBatchHandler) {
		this.rowBatchHandler = rowBatchHandler;
	}

	public void insertOneWithBatch(T t) {
		if (t == null) {
			log.warn("po must not be null!");
			return;
		}
		this.rowBatchHandler.insertWithBatch(Arrays.asList(t));
	}

	public void insertBatch(List<T> coll) {
		this.rowBatchHandler.insertWithBatch(coll);
	}
}
