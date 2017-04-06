/*
 * Copyright (C), 上海布鲁爱电子商务有限公司
 */
package com.thebeastshop.batch.handler;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;

import com.thebeastshop.batch.context.HandlerContext;
import com.thebeastshop.batch.enums.FileSavedCapacity;

/**
 * @author Paul-xiong
 * @date 2017年3月16日
 * @description
 */
public class FileRowBatchHandler<T> extends RowBatchHandler<T> {

	private File file;

	private String originFilePath;

	private Calendar fileDate = Calendar.getInstance();

	private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

	private FileSavedCapacity capacity = FileSavedCapacity.SINGLE;

	private File getFile() {
		if (capacity.equals(FileSavedCapacity.DAILY)) {
			Calendar now = Calendar.getInstance();
			if (now.get(Calendar.YEAR) > fileDate.get(Calendar.YEAR) || now.get(Calendar.MONTH) > fileDate.get(Calendar.MONTH) || now.get(Calendar.DAY_OF_MONTH) > fileDate.get(Calendar.DAY_OF_MONTH)) {
				fileDate = now;
				file = new File(originFilePath + "-" + simpleDateFormat.format(fileDate.getTime()));
			}
		}
		return file;
	}
	// -------------------------------------

	public FileRowBatchHandler(HandlerContext<T> context, File file, FileSavedCapacity capacity) {
		super(context);
		this.originFilePath = file.getAbsolutePath();
		if (capacity.equals(FileSavedCapacity.DAILY)) {
			this.file = new File(originFilePath + "-" + simpleDateFormat.format(fileDate.getTime()));
		} else {
			this.file = file;
		}
		this.capacity = capacity;
	}

	@Override
	public void rowBatch(List<T> batchList) {
		logger.info("开始批次插入文件");
		if (batchList != null && batchList.size() > 0) {
			OutputStream os = null;
			PrintWriter pw = null;
			try {
				os = new FileOutputStream(getFile(), true);
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

}
