/*
 * Copyright (C), 上海布鲁爱电子商务有限公司
 */
package com.litesalt.batch.handler;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.List;

/**
 * @author Paul-xiong
 * @date 2017年3月16日
 * @description
 */
public class FileRowBatchHandler<T> extends RowBatchHandler<T> {

	private File file;

	public FileRowBatchHandler(File file, long submitCapacity, Class<T> clazz) {
		super(submitCapacity, clazz);
		this.file = file;
	}

	@Override
	public void rowBatch(List<T> batchList) {
		logger.info("开始批次插入文件");
		if (batchList != null && batchList.size() > 0) {
			OutputStream os = null;
			PrintWriter pw = null;
			try {
				os = new FileOutputStream(file, true);
				pw = new PrintWriter(os, true);
				for (T t : batchList) {
					if (t != null) {
						pw.println(t);
					}
				}
				pw.flush();
			} catch (Exception e) {
				logger.error("批次插入文件异常: ", e);
			} finally {
				try {
					pw.close();
				} catch (Exception e2) {
					logger.error("批次插入文件异常: {}", e2);
				}
			}
		}
	}

	@Override
	public void aliasTable(String tableName) {
		// TODO Auto-generated method stub

	}

	@Override
	public void aliasField(String fieldName, String columnName) {
		// TODO Auto-generated method stub

	}

}
